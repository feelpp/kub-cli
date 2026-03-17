# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from kub_cli.cli import datasetApp, simulateApp


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
