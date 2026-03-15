<!--
SPDX-FileCopyrightText: 2026 University of Strasbourg
SPDX-FileContributor: Christophe Prud'homme
SPDX-FileContributor: Cemosis
SPDX-License-Identifier: Apache-2.0
-->

# Contributing

## Development setup

```bash
python -m pip install -e .[dev]
```

## Run tests

```bash
pytest
```

## Scope reminder

`kub-cli` is a thin wrapper around in-container apps. Keep new changes focused on:

- configuration resolution
- runtime orchestration
- user-facing wrapper ergonomics

Avoid reimplementing business logic that belongs in `kub-dataset`, `kub-simulate`, or `kub-dashboard` inside the image.

## Pull requests

- include tests for behavior changes
- update README when user-facing behavior changes
- keep dependencies minimal

## Releases

- Use SemVer (`MAJOR.MINOR.PATCH`) for `project.version`.
- Use matching Git tag format `vMAJOR.MINOR.PATCH`.
- PyPI publishing is handled by `.github/workflows/publish.yml` using GitHub environment `pypi`.
- Official publish source is `feelpp/kub-cli` and target PyPI org is `feelpp`.
