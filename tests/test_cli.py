# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest
from typer.testing import CliRunner

from kub_cli.cli import dashboardApp, datasetApp, simulateApp


@pytest.fixture
def cliRunner() -> CliRunner:
    return CliRunner()


def extractFlagValues(command: list[str], flag: str) -> list[str]:
    values: list[str] = []
    for index, token in enumerate(command):
        if token == flag and index + 1 < len(command):
            values.append(command[index + 1])
    return values


def testForwardingArgsWithDoubleDashApptainer(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "--runner",
            "apptainer",
            "--",
            "run",
            "case.yaml",
            "--mesh",
            "fine",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[-4:] == ["run", "case.yaml", "--mesh", "fine"]
    assert "--cemdb-root" in command
    assert "/cemdb" in command
    bindValues = extractFlagValues(command, "--bind")
    assert any(value.endswith(":/cemdb") for value in bindValues)


def testForwardingArgsWithoutDoubleDashDocker(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "push",
            "./data",
            "--tag",
            "baseline",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[0] == "/usr/bin/docker"
    assert command[1] == "run"
    assert command[-8:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-dataset",
        "--cemdb-root",
        "/cemdb",
        "push",
        "./data",
        "--tag",
        "baseline",
    ]
    volumeValues = extractFlagValues(command, "--volume")
    assert any(value.endswith(":/cemdb") for value in volumeValues)


def testBindOptionIsPassedToDockerAsVolume(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--bind",
            "/data:/data",
            "--bind",
            "/scratch:/scratch",
            "push",
            "/data/myset",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert "--volume" in command
    assert "/data:/data" in command
    assert "/scratch:/scratch" in command
    assert any(value.endswith(":/cemdb") for value in extractFlagValues(command, "--volume"))


def testDryRunPrintsDockerCommandAndSkipsExecution(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        dashboardApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--dry-run",
            "serve",
            "./results",
        ],
    )

    assert result.exit_code == 0
    assert called["value"] is False
    assert "docker run" in result.stdout
    assert "kub-dashboard --cemdb-root /cemdb serve ./results" in result.stdout


def testSelectedDockerRuntimeWithoutRunnerIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: None)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
        ],
    )

    assert result.exit_code == 2
    assert "unable to find docker runner" in result.output.lower()


def testSelectedApptainerRuntimeMissingImageIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingImage = tmp_path / "missing.sif"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        datasetApp,
        ["--runtime", "apptainer", "--image", str(missingImage), "push", "./data"],
    )

    assert result.exit_code == 2
    assert "Container image not found" in result.output


def testAutoRuntimeFallsBackToDocker(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingImage = tmp_path / "missing.sif"

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "auto",
            "--image",
            str(missingImage),
            "--",
            "run",
            "case.yaml",
        ],
        env={"KUB_IMAGE_DOCKER": "ghcr.io/feelpp/ktirio-urban-building:master"},
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[0] == "/usr/bin/docker"


def testInnerVersionOptionIsForwardedAfterFirstArgument(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "cemdb").mkdir()

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "pull-simulator",
            "--version",
            "0.2.0",
            "--cemdb-root",
            "cemdb",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "kub-cli 0." not in result.stdout

    command = captured["command"]  # type: ignore[assignment]
    assert command[-6:] == [
        "pull-simulator",
        "--version",
        "0.2.0",
        "--cemdb-root",
        "/cemdb",
        "--force",
    ]
    volumeValues = extractFlagValues(command, "--volume")
    assert f"{(tmp_path / 'cemdb').resolve()}:/cemdb" in volumeValues


def testExplicitWrapperCemdbRootIsMountedAndForwarded(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hostCemdb = tmp_path / "my-cemdb"
    hostCemdb.mkdir()

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(hostCemdb),
            "run",
            "case.yaml",
        ],
    )

    assert result.exit_code == 0

    command = captured["command"]  # type: ignore[assignment]
    volumeValues = extractFlagValues(command, "--volume")
    assert f"{hostCemdb.resolve()}:/cemdb" in volumeValues
    assert command[-4:] == ["--cemdb-root", "/cemdb", "run", "case.yaml"]


def testMissingWrapperCemdbRootIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingRoot = tmp_path / "missing-cemdb"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(missingRoot),
            "push",
            "./data",
        ],
    )

    assert result.exit_code == 2
    assert "CEMDB root path does not exist" in result.output
