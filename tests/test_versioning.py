# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path
import re

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
    (projectRoot / "CHANGELOG.md").write_text(
        (
            "# Changelog\n\n"
            "## Unreleased\n\n"
            "- Pending change\n\n"
            f"## {versionValue} - 2026-03-01\n\n"
            "- Previous release\n"
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
        releaseDate="2026-03-15",
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
    assert result.changelogUpdated is True

    changelogContent = (projectRoot / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## Unreleased" in changelogContent
    assert "## 1.2.4 - 2026-03-15" in changelogContent
    assert "- Pending change" in changelogContent


def testBumpProjectVersionDryRunDoesNotWrite(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "0.9.0")

    result = bumpProjectVersion(
        projectRoot=projectRoot,
        part="minor",
        toVersion=None,
        dryRun=True,
        releaseDate="2026-03-15",
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
    assert result.changelogUpdated is False

    changelogContent = (projectRoot / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## 0.9.0 - 2026-03-01" in changelogContent


def testBumpProjectVersionToExplicitValue(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "2.1.0")

    result = bumpProjectVersion(
        projectRoot=projectRoot,
        part="patch",
        toVersion="2.5.9",
        dryRun=False,
        releaseDate="2026-03-15",
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
    assert "git tag v3.1.0" in result.stdout
    assert "git push origin v3.1.0" in result.stdout
    assert 'gh release create v3.1.0 --generate-notes --title "v3.1.0"' in result.stdout
    assert "kub-cli thin wrapper" not in result.stdout

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "3.1.0"' in pyprojectContent

    changelogContent = (projectRoot / "CHANGELOG.md").read_text(encoding="utf-8")
    assert re.search(r"^## 3\.1\.0 - \d{4}-\d{2}-\d{2}$", changelogContent, re.MULTILINE)


def testMetaBumpCommandDryRun(cliRunner: CliRunner, tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "4.4.4")

    result = cliRunner.invoke(
        metaApp,
        ["bump", "patch", "--project-root", str(projectRoot), "--dry-run"],
    )

    assert result.exit_code == 0
    assert "Planned version: 4.4.4 -> 4.4.5" in result.stdout
    assert "git tag v4.4.5" in result.stdout
    assert "git push origin v4.4.5" in result.stdout
    assert 'gh release create v4.4.5 --generate-notes --title "v4.4.5"' in result.stdout

    pyprojectContent = (projectRoot / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "4.4.4"' in pyprojectContent


def testMetaGenerateShellCompletionBashIncludesAllTools(
    cliRunner: CliRunner,
) -> None:
    result = cliRunner.invoke(
        metaApp,
        ["generate-shell-completion", "bash"],
    )

    assert result.exit_code == 0
    assert "_kub_cli_completion()" in result.stdout
    assert "_kub_dataset_completion()" in result.stdout
    assert "_kub_simulate_completion()" in result.stdout
    assert "_kub_dashboard_completion()" in result.stdout
    assert "_kub_img_completion()" in result.stdout
    assert "-F _kub_cli_completion kub-cli" in result.stdout
    assert "-F _kub_dataset_completion kub-dataset" in result.stdout
    assert "-F _kub_simulate_completion kub-simulate" in result.stdout
    assert "-F _kub_dashboard_completion kub-dashboard" in result.stdout
    assert "-F _kub_img_completion kub-img" in result.stdout
    assert "_KUB_CLI_COMPLETE=complete_bash" in result.stdout
    assert "_KUB_IMG_COMPLETE=complete_bash" in result.stdout
    assert "IFS=',' read type value" in result.stdout


def testMetaGenerateShellCompletionRejectsUnsupportedShell(
    cliRunner: CliRunner,
) -> None:
    result = cliRunner.invoke(
        metaApp,
        ["generate-shell-completion", "tcsh"],
    )

    assert result.exit_code == 2
    assert "Unsupported shell 'tcsh'" in result.output


def testMetaDoctorJsonReportsCapabilities(
    cliRunner: CliRunner,
) -> None:
    result = cliRunner.invoke(
        metaApp,
        [
            "doctor",
            "--runtime",
            "docker",
            "--runner",
            "/bin/echo",
            "--json",
            "--no-cache",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["cacheHit"] is False
    assert payload["requiredFailureCount"] == 0
    checks = payload["checks"]
    assert any(
        check["name"] == "runtime-resolution" and check["status"] == "ok"
        for check in checks
    )


def testMetaDoctorFailsWhenRuntimeCannotResolve(
    cliRunner: CliRunner,
    tmp_path: Path,
) -> None:
    missingRunner = tmp_path / "missing-docker"
    result = cliRunner.invoke(
        metaApp,
        [
            "doctor",
            "--runtime",
            "docker",
            "--runner",
            str(missingRunner),
            "--json",
            "--no-cache",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["requiredFailureCount"] == 1
    checks = payload["checks"]
    assert any(
        check["name"] == "runtime-resolution" and check["status"] == "fail"
        for check in checks
    )


def testBumpWithoutUnreleasedSectionRaises(tmp_path: Path) -> None:
    projectRoot = tmp_path / "repo"
    writeProjectLayout(projectRoot, "1.0.0")
    (projectRoot / "CHANGELOG.md").write_text("# Changelog\n\n## 1.0.0 - 2026-03-01\n", encoding="utf-8")

    with pytest.raises(KubCliError, match="missing '## Unreleased' section"):
        bumpProjectVersion(
            projectRoot=projectRoot,
            part="patch",
            toVersion=None,
            dryRun=False,
            releaseDate="2026-03-15",
        )
