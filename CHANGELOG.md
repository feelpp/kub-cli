<!--
SPDX-FileCopyrightText: 2026 University of Strasbourg
SPDX-FileContributor: Christophe Prud'homme
SPDX-FileContributor: Cemosis
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

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
