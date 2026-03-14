# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from kub_cli.cli import metaApp
from kub_cli.errors import KubCliError
from kub_cli.versioning import bumpProjectVersion


@pytest.fixture
def cliRunner() -> CliRunner:
    return CliRunner()


def writeProjectLayout(projectRoot: Path, versionValue: str) -> None:
    srcDir = projectRoot / "src" / "kub_cli"
    srcDir.mkdir(parents=True, exist_ok=True)

    (projectRoot / "pyproject.toml").write_text(
        (
            "[project]\n"
            'name = "kub-cli"\n'
            f'version = "{versionValue}"\n'
        ),
        encoding="utf-8",
    )
    (srcDir / "__init__.py").write_text(
        (
            '"""kub-cli package."""\n\n'
            'from importlib.metadata import PackageNotFoundError, version\n\n'
            "try:\n"
            '    __version__ = version("kub-cli")\n'
            "except PackageNotFoundError:\n"
            f'    __version__ = "{versionValue}"\n'
        ),
        encoding="utf-8",
    )


def testBumpProjectVersionPatchUpdatesFiles(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "1.2.3")

    result = bumpProjectVersion(
        projectRoot=projectRoot,
        part="patch",
        toVersion=None,
        dryRun=False,
    )

    assert result.oldVersion == "1.2.3"
    assert result.newVersion == "1.2.4"
    assert result.changed is True

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    initContent = (projectRoot / "src" / "kub_cli" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert 'version = "1.2.4"' in pyprojectContent
    assert '__version__ = "1.2.4"' in initContent


def testBumpProjectVersionDryRunDoesNotWrite(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "0.9.0")

    result = bumpProjectVersion(
        projectRoot=projectRoot,
        part="minor",
        toVersion=None,
        dryRun=True,
    )

    assert result.oldVersion == "0.9.0"
    assert result.newVersion == "0.10.0"
    assert result.changed is True

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    initContent = (projectRoot / "src" / "kub_cli" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert 'version = "0.9.0"' in pyprojectContent
    assert '__version__ = "0.9.0"' in initContent


def testBumpProjectVersionToExplicitValue(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "2.1.0")

    result = bumpProjectVersion(
        projectRoot=projectRoot,
        part="patch",
        toVersion="2.5.9",
        dryRun=False,
    )

    assert result.oldVersion == "2.1.0"
    assert result.newVersion == "2.5.9"

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "2.5.9"' in pyprojectContent


def testBumpProjectVersionInvalidPartRaises(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "1.0.0")

    with pytest.raises(KubCliError, match="Invalid bump part"):
        bumpProjectVersion(
            projectRoot=projectRoot,
            part="build",
            toVersion=None,
            dryRun=False,
        )


def testMetaBumpCommandUpdatesVersion(cliRunner: CliRunner, tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "3.0.1")

    result = cliRunner.invoke(
        metaApp,
        ["bump", "minor", "--project-root", str(projectRoot)],
    )

    assert result.exit_code == 0
    assert "Updated version: 3.0.1 -> 3.1.0" in result.stdout
    assert "kub-cli thin wrapper" not in result.stdout

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "3.1.0"' in pyprojectContent


def testMetaBumpCommandDryRun(cliRunner: CliRunner, tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "4.4.4")

    result = cliRunner.invoke(
        metaApp,
        ["bump", "patch", "--project-root", str(projectRoot), "--dry-run"],
    )

    assert result.exit_code == 0
    assert "Planned version: 4.4.4 -> 4.4.5" in result.stdout

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "4.4.4"' in pyprojectContent
