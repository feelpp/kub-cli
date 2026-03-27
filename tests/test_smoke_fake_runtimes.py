# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest
from typer.testing import CliRunner

from kub_cli.cli import datasetApp, simulateApp
from kub_cli.img_cli import imgApp
from kub_cli.img_tools import KubImgManager


@pytest.fixture
def cliRunner() -> CliRunner:
    return CliRunner()


def writeExecutable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def testDockerWrapperSmokeWithFakeRunner(
    cliRunner: CliRunner,
    tmp_path: Path,
) -> None:
    fakeBin = tmp_path / "bin"
    fakeBin.mkdir()
    dockerLog = tmp_path / "docker-args.log"
    cemdbRoot = tmp_path / "cemdb"
    cemdbRoot.mkdir()

    writeExecutable(
        fakeBin / "docker",
        (
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$@\" > \"{dockerLog}\"\n"
            "exit 0\n"
        ),
    )

    env = dict(os.environ)
    env["PATH"] = f"{fakeBin}:{env.get('PATH', '')}"

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(cemdbRoot),
            "push",
            "./data",
        ],
        env=env,
    )

    assert result.exit_code == 0
    args = dockerLog.read_text(encoding="utf-8").splitlines()

    assert args[0] == "run"
    assert "ghcr.io/feelpp/ktirio-urban-building:master" in args
    assert "kub-dataset" in args
    assert "push" in args
    assert "./data" in args
    assert "--volume" in args
    assert f"{cemdbRoot.resolve()}:/cemdb" in args


def testApptainerWrapperSmokeWithFakeRunner(
    cliRunner: CliRunner,
    tmp_path: Path,
) -> None:
    fakeBin = tmp_path / "bin"
    fakeBin.mkdir()
    apptainerRunLog = tmp_path / "apptainer-run-args.log"

    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")
    cemdbRoot = tmp_path / "cemdb"
    cemdbRoot.mkdir()

    writeExecutable(
        fakeBin / "apptainer",
        (
            "#!/bin/sh\n"
            "if [ \"$1\" = \"inspect\" ] && [ \"$2\" = \"--list-apps\" ]; then\n"
            "  printf 'kub-dataset\\nkub-simulate\\nkub-dashboard\\n'\n"
            "  exit 0\n"
            "fi\n"
            f"printf '%s\\n' \"$@\" > \"{apptainerRunLog}\"\n"
            "exit 0\n"
        ),
    )

    env = dict(os.environ)
    env["PATH"] = f"{fakeBin}:{env.get('PATH', '')}"

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "--cemdb-root",
            str(cemdbRoot),
            "status",
            "arz",
        ],
        env=env,
    )

    assert result.exit_code == 0
    args = apptainerRunLog.read_text(encoding="utf-8").splitlines()

    assert args[0] == "run"
    assert "--app" in args
    assert "kub-simulate" in args
    assert str(imagePath) in args
    assert "--config" in args
    assert "/cemdb/.kub-simulate.toml" in args
    assert "status" in args
    assert "arz" in args


def testTutorialImageCommandsWorkWithLocalApptainerImage(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub-master.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    def fakeRunCommand(self, command, captureOutput, dryRun, runtimeConfig=None):  # type: ignore[no-untyped-def]
        if command[:3] == ["/usr/bin/apptainer", "inspect", "--list-apps"]:
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="kub-simulate\nkub-dataset\nkub-dashboard\n",
                stderr="",
            )

        if command[:3] == ["/usr/bin/apptainer", "inspect", "--labels"]:
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="org.opencontainers.image.version: master\n",
                stderr="",
            )

        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    pathResult = cliRunner.invoke(
        imgApp,
        [
            "path",
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
        ],
    )

    assert pathResult.exit_code == 0
    assert pathResult.output.strip() == str(imagePath)

    infoResult = cliRunner.invoke(
        imgApp,
        [
            "info",
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "--json",
        ],
    )

    assert infoResult.exit_code == 0
    payload = json.loads(infoResult.output)
    assert payload["runtime"] == "apptainer"
    assert payload["image"] == str(imagePath)
    assert payload["apps"] == ["kub-simulate", "kub-dataset", "kub-dashboard"]


def testTutorialSimulateCommandsWorkInEmptyCemdb(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub-master.sif"
    imagePath.write_text("dummy", encoding="utf-8")
    cemdbRoot = tmp_path / "cemdb"
    cemdbRoot.mkdir()
    rootConfig = tmp_path / ".kub-simulate.toml"
    nestedConfig = tmp_path / "cemdb" / ".kub-simulate.toml"
    configState = {
        "cemdb_root": "cemdb/locations",
        "apptainer-local": {"launcher": "local", "runtime": "apptainer"},
        "apptainer-slurm": {
            "launcher": "slurm",
            "partition": "public",
            "runtime": "apptainer",
        },
    }

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    def writeConfig(
    ) -> None:
        rootConfig.write_text(
            "\n".join(
                [
                    "# kub-simulate user configuration",
                    "",
                    "[defaults]",
                    f"cemdb_root = {json.dumps(configState['cemdb_root'])}",
                    "",
                    "[profiles.apptainer-local]",
                    f"launcher = {json.dumps(configState['apptainer-local']['launcher'])}",
                    f"runtime = {json.dumps(configState['apptainer-local']['runtime'])}",
                    "",
                    "[profiles.apptainer-slurm]",
                    f"launcher = {json.dumps(configState['apptainer-slurm']['launcher'])}",
                    f"partition = {json.dumps(configState['apptainer-slurm']['partition'])}",
                    f"runtime = {json.dumps(configState['apptainer-slurm']['runtime'])}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def fakeRun(command, check=False, env=None, capture_output=False, text=False, timeout=None):  # type: ignore[no-untyped-def]
        if len(command) >= 3 and command[1] == "inspect" and command[2] == "--list-apps":
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="kub-simulate\nkub-dataset\nkub-dashboard\n",
                stderr="",
            )

        innerArgs = list(command)
        imageIndex = innerArgs.index(str(imagePath))
        forwarded = innerArgs[imageIndex + 1 :]
        if forwarded[:2] != ["--config", "/cemdb/.kub-simulate.toml"]:
            raise AssertionError(f"Unexpected forwarded args: {forwarded}")

        innerCommand = forwarded[2:]

        if innerCommand[:2] == ["config", "init"]:
            writeConfig()
        elif innerCommand[:2] == ["config", "set"]:
            key = innerCommand[2]
            value = innerCommand[3]
            profileTarget = None
            if "--profile-target" in innerCommand:
                profileTarget = innerCommand[innerCommand.index("--profile-target") + 1]

            if profileTarget is None and key == "cemdb_root":
                configState["cemdb_root"] = value
            elif profileTarget == "apptainer-local":
                configState["apptainer-local"][key] = value
            elif profileTarget == "apptainer-slurm":
                configState["apptainer-slurm"][key] = value

            writeConfig()
        elif innerCommand[:2] == ["config", "show"]:
            print(rootConfig.read_text(encoding="utf-8"), end="")
        elif innerCommand[:1] not in (["preprocess"], ["run"], ["status"]):
            raise AssertionError(f"Unexpected inner command: {innerCommand}")

        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    commands = [
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "init",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "cemdb_root",
            "cemdb/locations",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "launcher",
            "slurm",
            "--profile-target",
            "apptainer-slurm",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "partition",
            "public",
            "--profile-target",
            "apptainer-slurm",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "runtime",
            "apptainer",
            "--profile-target",
            "apptainer-slurm",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "launcher",
            "local",
            "--profile-target",
            "apptainer-local",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "set",
            "runtime",
            "apptainer",
            "--profile-target",
            "apptainer-local",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "config",
            "show",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "preprocess",
            "arz",
            "--version",
            "0.1.0",
            "--profile",
            "apptainer-local",
            "--partitions",
            "32",
            "64",
            "128",
            "--dry-run",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "run",
            "arz",
            "--version",
            "0.1.0",
            "--profile",
            "apptainer-local",
            "--np",
            "128",
            "--dry-run",
        ],
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "status",
            "arz",
            "--version",
            "0.1.0",
            "--last",
            "10",
        ],
    ]

    lastResult = None
    for command in commands:
        lastResult = cliRunner.invoke(simulateApp, command)
        assert lastResult.exit_code == 0, lastResult.output

    assert lastResult is not None
    assert rootConfig.exists()
    assert nestedConfig.exists()
    configText = rootConfig.read_text(encoding="utf-8")
    assert 'cemdb_root = "cemdb/locations"' in configText
    assert 'launcher = "local"' in configText
    assert 'partition = "public"' in configText
    assert 'runtime = "apptainer"' in configText
    assert nestedConfig.read_text(encoding="utf-8") == configText
