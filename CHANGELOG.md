<!--
SPDX-FileCopyrightText: 2026 University of Strasbourg
SPDX-FileContributor: Christophe Prud'homme
SPDX-FileContributor: Cemosis
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

## Unreleased

## 0.9.0 - 2026-03-17
- Added `kub-cli bump` command for SemVer version updates in `pyproject.toml` and fallback package version
- Added GitHub Actions workflow for PyPI publishing via GitHub environment `pypi` (`.github/workflows/publish.yml`)
- Added release validation script enforcing `vMAJOR.MINOR.PATCH` tags and tag/version matching
- Restricted publish workflow to official `feelpp/kub-cli` repository and documented PyPI organization target `feelpp`
- Fixed wrapper argument forwarding so app flags like `--version` after the first inner command token are passed to in-container apps
- Updated `kub-cli bump` to automatically rotate `CHANGELOG.md` (`Unreleased` -> released section with date)
- Added automatic `/cemdb` bind handling for wrapped apps: current directory is mounted by default and `--cemdb-root` is forwarded as `/cemdb`
- Improved Docker bind usability: default Docker execution now uses host UID:GID, and missing `--cemdb-root` directories are auto-created
- Added writable dataset-config defaults in containers: `HOME=/cemdb`, `KUB_CONFIG=/cemdb/.kub/config.toml`, and auto-creation of `.kub` under cemdb root
- Default container workdir is now `/cemdb` (unless `--pwd` is set), preventing writes to unwritable image home directories

## 0.1.0 - 2026-03-14

- Initial production-ready release
- Added `kub-dataset`, `kub-simulate`, `kub-dashboard` wrapper commands
- Added `kub-img` command with `pull`, `info`, `apps`, and `path` actions
- Added multi-runtime execution support (`auto`, `apptainer`, `docker`)
- Added runtime-aware image configuration (`KUB_IMAGE_DOCKER`, `KUB_IMAGE_APPTAINER`)
- Added Docker command builder and runtime resolution policy for auto mode
- Added Apptainer ORAS derivation helper (`docker-ref:tag` -> `oras://...:tag-sif`)
- Added internal kub-img subprocess integration helpers for pull/info workflows
- Set default runtime to `auto` with Apptainer-first then Docker detection
- Added default master image references:
  - Docker: `ghcr.io/feelpp/ktirio-urban-building:master`
  - Apptainer: `oras://ghcr.io/feelpp/ktirio-urban-building:master-sif`
- Added config resolution with CLI/env/project/user precedence
- Added dry-run and verbose modes
- Added unit test suite for config, runtime, and CLI behavior
