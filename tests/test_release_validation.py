# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def writePyproject(path: Path, versionValue: str) -> None:
    path.write_text(
        (
            "[project]\n"
            'name = "kub-cli"\n'
            f'version = "{versionValue}"\n'
        ),
        encoding="utf-8",
    )


def scriptPath() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts" / "validate_release_tag.py"


def testValidateReleaseTagSuccess(tmp_path: Path) -> None:
    pyprojectPath = tmp_path / "pyproject.toml"
    writePyproject(pyprojectPath, "1.2.3")

    process = subprocess.run(
        [
            sys.executable,
            str(scriptPath()),
            "--tag",
            "v1.2.3",
            "--pyproject",
            str(pyprojectPath),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == 0
    assert "Validated release tag v1.2.3" in process.stdout


def testValidateReleaseTagRejectsInvalidTag(tmp_path: Path) -> None:
    pyprojectPath = tmp_path / "pyproject.toml"
    writePyproject(pyprojectPath, "1.2.3")

    process = subprocess.run(
        [
            sys.executable,
            str(scriptPath()),
            "--tag",
            "release-1.2.3",
            "--pyproject",
            str(pyprojectPath),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == 2
    assert "must start with 'v'" in process.stderr


def testValidateReleaseTagRejectsVersionMismatch(tmp_path: Path) -> None:
    pyprojectPath = tmp_path / "pyproject.toml"
    writePyproject(pyprojectPath, "1.2.3")

    process = subprocess.run(
        [
            sys.executable,
            str(scriptPath()),
            "--tag",
            "v1.2.4",
            "--pyproject",
            str(pyprojectPath),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == 2
    assert "Tag version does not match pyproject version" in process.stderr
