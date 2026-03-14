# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Version bump utilities for kub-cli development workflows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from .errors import KubCliError


SEMVER_PATTERN = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
PYPROJECT_VERSION_PATTERN = re.compile(
    r"(^version\s*=\s*\")(\d+\.\d+\.\d+)(\"\s*$)",
    re.MULTILINE,
)
INIT_FALLBACK_VERSION_PATTERN = re.compile(
    r"(^\s*__version__\s*=\s*\")(\d+\.\d+\.\d+)(\"\s*$)",
    re.MULTILINE,
)


@dataclass(frozen=True)
class SemanticVersion:
    """Semantic version components."""

    major: int
    minor: int
    patch: int

    def toString(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class BumpResult:
    """Result of version bump planning/execution."""

    oldVersion: str
    newVersion: str
    pyprojectPath: Path
    initPath: Path
    changed: bool


def parseSemanticVersion(rawValue: str) -> SemanticVersion:
    normalized = rawValue.strip()
    match = SEMVER_PATTERN.match(normalized)
    if match is None:
        raise KubCliError(
            f"Invalid version '{rawValue}'. Expected semantic version MAJOR.MINOR.PATCH."
        )

    return SemanticVersion(
        major=int(match.group(1)),
        minor=int(match.group(2)),
        patch=int(match.group(3)),
    )


def bumpSemanticVersion(currentVersion: SemanticVersion, part: str) -> SemanticVersion:
    normalizedPart = part.strip().lower()

    if normalizedPart == "major":
        return SemanticVersion(
            major=currentVersion.major + 1,
            minor=0,
            patch=0,
        )

    if normalizedPart == "minor":
        return SemanticVersion(
            major=currentVersion.major,
            minor=currentVersion.minor + 1,
            patch=0,
        )

    if normalizedPart == "patch":
        return SemanticVersion(
            major=currentVersion.major,
            minor=currentVersion.minor,
            patch=currentVersion.patch + 1,
        )

    raise KubCliError(
        f"Invalid bump part '{part}'. Use one of: major, minor, patch."
    )


def bumpProjectVersion(
    *,
    projectRoot: Path,
    part: str,
    toVersion: str | None,
    dryRun: bool,
) -> BumpResult:
    pyprojectPath = projectRoot / "pyproject.toml"
    initPath = projectRoot / "src" / "kub_cli" / "__init__.py"

    oldVersion = readPyprojectVersion(pyprojectPath)
    oldSemanticVersion = parseSemanticVersion(oldVersion)

    if toVersion is not None:
        newVersion = parseSemanticVersion(toVersion).toString()
    else:
        newVersion = bumpSemanticVersion(oldSemanticVersion, part).toString()

    changed = oldVersion != newVersion

    if not dryRun and changed:
        replaceVersionInFile(
            filePath=pyprojectPath,
            pattern=PYPROJECT_VERSION_PATTERN,
            newVersion=newVersion,
            valueLabel="pyproject version",
        )
        replaceVersionInFile(
            filePath=initPath,
            pattern=INIT_FALLBACK_VERSION_PATTERN,
            newVersion=newVersion,
            valueLabel="__init__ fallback version",
        )

    return BumpResult(
        oldVersion=oldVersion,
        newVersion=newVersion,
        pyprojectPath=pyprojectPath,
        initPath=initPath,
        changed=changed,
    )


def readPyprojectVersion(pyprojectPath: Path) -> str:
    if not pyprojectPath.exists():
        raise KubCliError(f"pyproject.toml not found at '{pyprojectPath}'.")

    content = pyprojectPath.read_text(encoding="utf-8")
    match = PYPROJECT_VERSION_PATTERN.search(content)

    if match is None:
        raise KubCliError(
            f"Unable to locate project version in '{pyprojectPath}'."
        )

    return match.group(2)


def replaceVersionInFile(
    *,
    filePath: Path,
    pattern: re.Pattern[str],
    newVersion: str,
    valueLabel: str,
) -> None:
    if not filePath.exists():
        raise KubCliError(f"Expected file not found: '{filePath}'.")

    content = filePath.read_text(encoding="utf-8")

    def replacement(match: re.Match[str]) -> str:
        return f"{match.group(1)}{newVersion}{match.group(3)}"

    updatedContent, count = pattern.subn(replacement, content, count=1)

    if count != 1:
        raise KubCliError(
            f"Unable to update {valueLabel} in '{filePath}'."
        )

    filePath.write_text(updatedContent, encoding="utf-8")
