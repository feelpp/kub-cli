# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest
from typer.testing import CliRunner

from kub_cli.img_cli import imgApp
from kub_cli.img_tools import KubImgManager


@pytest.fixture
def cliRunner() -> CliRunner:
    return CliRunner()


def testApptainerPullDryRunUsesOrasSource(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    targetImage = tmp_path / "kub.sif"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        imgApp,
        [
            "pull",
            "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif",
            "--runtime",
            "apptainer",
            "--image",
            str(targetImage),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "apptainer pull" in result.output
    assert "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif" in result.output
    assert "docker://" not in result.output


def testApptainerPullDryRunWorksWithDefaults(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        imgApp,
        [
            "pull",
            "--runtime",
            "apptainer",
            "--dry-run",
        ],
    )

    expectedImage = str((tmp_path / "kub-master.sif").resolve())
    assert result.exit_code == 0
    assert "apptainer pull" in result.output
    assert expectedImage in result.output
    assert "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif" in result.output


def testDockerPullCommandBuildsExpectedArguments(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRunCommand(self, command, captureOutput, dryRun, runtimeConfig=None):  # type: ignore[no-untyped-def]
        captured.setdefault("commands", []).append(command)
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "pull",
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
        ],
    )

    assert result.exit_code == 0
    commands = captured["commands"]  # type: ignore[assignment]
    assert commands[0] == [
        "/usr/bin/docker",
        "pull",
        "ghcr.io/feelpp/ktirio-urban-building:master",
    ]


def testDockerInfoJsonOutput(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    def fakeRunCommand(self, command, captureOutput, dryRun, runtimeConfig=None):  # type: ignore[no-untyped-def]
        if command[:3] == ["/usr/bin/docker", "image", "inspect"]:
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout='[{"Id": "sha256:abc", "RepoTags": ["ghcr.io/x/y:tag"]}]',
                stderr="",
            )

        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "info",
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--json",
        ],
    )

    assert result.exit_code == 0

    payload = json.loads(result.output)
    assert payload["runtime"] == "docker"
    assert payload["image"] == "ghcr.io/feelpp/ktirio-urban-building:master"
    assert payload["inspect"][0]["Id"] == "sha256:abc"


def testAppsCommandWithDockerRuntimeIsRejected(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    result = cliRunner.invoke(
        imgApp,
        [
            "apps",
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
        ],
    )

    assert result.exit_code == 2
    assert "Apptainer runtime" in result.output


def testInfoCommandMissingApptainerImageIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingImage = tmp_path / "missing.sif"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        imgApp,
        ["info", "--runtime", "apptainer", "--image", str(missingImage)],
    )

    assert result.exit_code == 2
    assert "Container image not found" in result.output


def testApptainerPullRejectsDockerSource(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        imgApp,
        [
            "pull",
            "docker://ghcr.io/feelpp/ktirio-urban-building:master",
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
        ],
    )

    assert result.exit_code == 2
    assert "must use oras://" in result.output


def testPathAutoResolvesToDockerWhenApptainerUnavailable(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fakeWhich(name: str) -> str | None:
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    result = cliRunner.invoke(
        imgApp,
        [
            "path",
            "--runtime",
            "auto",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "ghcr.io/feelpp/ktirio-urban-building:master"


def testPathAutoFallsBackToDockerWhenApptainerProbeFails(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    def fakeProbe(runnerPath: str, *, runtimeName: str) -> str | None:
        if runtimeName == "apptainer":
            return (
                "startup probe exited with code 255: "
                "FATAL: While initializing: couldn't parse configuration file "
                "/etc/apptainer/apptainer.conf"
            )
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)
    monkeypatch.setattr("kub_cli.runtime.probeRunnerExecutable", fakeProbe)

    result = cliRunner.invoke(
        imgApp,
        [
            "path",
            "--runtime",
            "auto",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "ghcr.io/feelpp/ktirio-urban-building:master"


def testAppsAutoRejectsWhenRuntimeResolvesToDocker(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fakeWhich(name: str) -> str | None:
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    result = cliRunner.invoke(
        imgApp,
        [
            "apps",
            "--runtime",
            "auto",
        ],
    )

    assert result.exit_code == 2
    assert "only available with Apptainer runtime" in result.output


def testApptainerLoginBuildsRegistryCommand(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "apptainer",
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == [
        "/usr/bin/apptainer",
        "registry",
        "login",
        "--username",
        "alice",
        "docker://ghcr.io",
    ]
    assert captured["inputText"] is None
    assert "Using Apptainer registry login for GHCR" in result.output
    assert "runtime will prompt for password/token" in result.output


def testDockerLoginBuildsLoginCommand(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "docker",
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == ["/usr/bin/docker", "login", "-u", "alice", "ghcr.io"]
    assert captured["inputText"] is None
    assert "Using Docker login for GHCR" in result.output
    assert "runtime will prompt for password/token" in result.output


def testLoginDefaultsToAutoAndFallsBackToDockerWhenApptainerUnavailable(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fakeWhich(name: str) -> str | None:
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == ["/usr/bin/docker", "login", "-u", "alice", "ghcr.io"]
    assert captured["inputText"] is None


def testLoginDefaultsToAutoAndPrefersApptainerWhenAvailable(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == [
        "/usr/bin/apptainer",
        "registry",
        "login",
        "--username",
        "alice",
        "docker://ghcr.io",
    ]
    assert captured["inputText"] is None


def testLoginAutoWithExplicitDockerRunnerUsesDockerLogin(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runnerPath = tmp_path / "docker"
    runnerPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runnerPath.chmod(0o755)

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "auto",
            "--runner",
            str(runnerPath),
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == [str(runnerPath), "login", "-u", "alice", "ghcr.io"]
    assert captured["inputText"] is None
    assert "Using Docker login for GHCR" in result.output


def testLoginAutoWithAmbiguousRunnerRequiresExplicitRuntime(
    cliRunner: CliRunner,
    tmp_path: Path,
) -> None:
    runnerPath = tmp_path / "runtime-wrapper"
    runnerPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runnerPath.chmod(0o755)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "auto",
            "--runner",
            str(runnerPath),
            "--username",
            "alice",
        ],
    )

    assert result.exit_code == 2
    assert "Unable to infer runtime from --runner" in result.output


def testLoginPromptsForUsernameWhenMissing(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "docker",
        ],
        input="alice\n",
    )

    assert result.exit_code == 0
    assert "GHCR username" in result.output
    assert captured["command"] == ["/usr/bin/docker", "login", "-u", "alice", "ghcr.io"]
    assert captured["inputText"] is None


def testDockerLoginWithTokenUsesPasswordStdin(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "docker",
            "--username",
            "alice",
            "--token",
            "secret-token",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == [
        "/usr/bin/docker",
        "login",
        "-u",
        "alice",
        "--password-stdin",
        "ghcr.io",
    ]
    assert captured["inputText"] == "secret-token\n"
    assert "sent via stdin" in result.output


def testApptainerLoginWithPasswordUsesPasswordStdin(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRunCommand(  # type: ignore[no-untyped-def]
        self,
        command,
        captureOutput,
        dryRun,
        runtimeConfig=None,
        inputText=None,
    ):
        captured["command"] = command
        captured["inputText"] = inputText
        return 0

    monkeypatch.setattr(KubImgManager, "runCommand", fakeRunCommand)

    result = cliRunner.invoke(
        imgApp,
        [
            "login",
            "--runtime",
            "apptainer",
            "--username",
            "alice",
            "--password",
            "secret-token",
        ],
    )

    assert result.exit_code == 0
    assert captured["command"] == [
        "/usr/bin/apptainer",
        "registry",
        "login",
        "--username",
        "alice",
        "--password-stdin",
        "docker://ghcr.io",
    ]
    assert captured["inputText"] == "secret-token\n"
