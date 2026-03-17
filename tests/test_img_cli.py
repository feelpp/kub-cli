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
