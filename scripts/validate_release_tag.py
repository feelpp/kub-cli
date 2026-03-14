#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Validate release tag format and match with pyproject version."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python < 3.11
    import tomli as tomllib


SEMVER_PATTERN = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def fail(message: str) -> int:
    print(f"Error: {message}", file=sys.stderr)
    return 2


def normalizeTagValue(rawTag: str) -> str:
    tagValue = rawTag.strip()
    if tagValue.startswith("refs/tags/"):
        tagValue = tagValue[len("refs/tags/") :]
    return tagValue


def extractVersionFromTag(tagValue: str) -> str:
    normalizedTag = normalizeTagValue(tagValue)
    if not normalizedTag.startswith("v"):
        raise ValueError(
            f"Release tag '{normalizedTag}' must start with 'v' (for example: v0.2.1)."
        )

    versionValue = normalizedTag[1:]
    if SEMVER_PATTERN.match(versionValue) is None:
        raise ValueError(
            f"Release tag '{normalizedTag}' is not valid SemVer (expected vMAJOR.MINOR.PATCH)."
        )

    return versionValue


def readProjectVersion(pyprojectPath: Path) -> str:
    if not pyprojectPath.exists():
        raise ValueError(f"pyproject.toml not found at '{pyprojectPath}'.")

    data = tomllib.loads(pyprojectPath.read_text(encoding="utf-8"))
    projectTable = data.get("project")
    if not isinstance(projectTable, dict):
        raise ValueError("Missing [project] table in pyproject.toml.")

    versionValue = projectTable.get("version")
    if not isinstance(versionValue, str):
        raise ValueError("Missing string project.version in pyproject.toml.")

    if SEMVER_PATTERN.match(versionValue.strip()) is None:
        raise ValueError(
            f"project.version '{versionValue}' is not SemVer (expected MAJOR.MINOR.PATCH)."
        )

    return versionValue.strip()


def buildParser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate release tag (vMAJOR.MINOR.PATCH) and pyproject version match."
    )
    parser.add_argument(
        "--tag",
        default=os.environ.get("GITHUB_REF_NAME", ""),
        help=(
            "Tag to validate (for example: v0.2.1). "
            "Defaults to GITHUB_REF_NAME if set."
        ),
    )
    parser.add_argument(
        "--pyproject",
        default="pyproject.toml",
        help="Path to pyproject.toml (default: pyproject.toml).",
    )
    return parser


def main() -> int:
    parser = buildParser()
    args = parser.parse_args()

    tagArg = str(args.tag).strip()
    if not tagArg:
        return fail("No tag provided. Pass --tag vMAJOR.MINOR.PATCH.")

    try:
        tagVersion = extractVersionFromTag(tagArg)
        projectVersion = readProjectVersion(Path(str(args.pyproject)))
    except ValueError as error:
        return fail(str(error))

    if tagVersion != projectVersion:
        return fail(
            "Tag version does not match pyproject version: "
            f"tag={tagVersion}, project.version={projectVersion}."
        )

    print(f"Validated release tag v{tagVersion} against project.version {projectVersion}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
