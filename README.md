<!--
SPDX-FileCopyrightText: 2026 University of Strasbourg
SPDX-FileContributor: Christophe Prud'homme
SPDX-FileContributor: Cemosis
SPDX-License-Identifier: Apache-2.0
-->

# kub-cli

`kub-cli` is a thin Python client that exposes stable native commands for KUB tools while executing the real logic inside container images.

- `kub-dataset`
- `kub-simulate`
- `kub-dashboard`
- `kub-img`

The wrapper does not reimplement dataset/simulation/dashboard business logic. It resolves configuration, builds runtime commands, and executes them.

License: Apache-2.0.

## Why this exists

`kub-cli` provides a consistent command-line UX across laptops, workstations, CI, and HPC environments.

## Runtime model

Supported runtimes:

- `apptainer`
- `docker`
- `auto`

Policy:

- Apptainer remains the preferred runtime for HPC / Slurm / MPI-oriented execution.
- Docker is supported for local/workstation/CI usage.
- `auto` prefers Apptainer when configured and available, then falls back to Docker.

Default behavior:

- Default runtime is `auto`.
- In `auto`, kub-cli checks Apptainer first, then Docker.
- If neither runtime is installed, kub-cli reports how to install them.

## Installation

### From source

```bash
python -m pip install .
```

### Editable install for development

```bash
python -m pip install -e .[dev]
```

## Quick start

Set runtime and images:

```bash
export KUB_RUNTIME=auto
export KUB_IMAGE_DOCKER=ghcr.io/feelpp/ktirio-urban-building:master
export KUB_IMAGE_APPTAINER=/path/to/ktirio-urban-building.sif
```

Then use wrapper commands:

```bash
kub-simulate --runtime docker -- run case.yaml
kub-dataset --runtime apptainer -- push ./data
kub-dashboard --runtime auto -- serve ./results
```

## Command behavior

For Apptainer runtime:

```text
apptainer run [common options] --app <wrapped-app> <local-sif-image> [forwarded args...]
```

For Docker runtime:

```text
docker run --rm [common options] <docker-image> <wrapped-app> [forwarded args...]
```

Mappings:

- `kub-dataset` -> `kub-dataset`
- `kub-simulate` -> `kub-simulate`
- `kub-dashboard` -> `kub-dashboard`

All non-wrapper arguments are forwarded transparently to the in-container app.

## Wrapper options

Available on all three wrapper commands:

- `--runtime {auto,apptainer,docker}`
- `--image IMAGE`
- `--bind SRC:DST` (repeatable)
- `--pwd PATH`
- `--runner PATH`
- `--apptainer-flag FLAG` (repeatable)
- `--docker-flag FLAG` (repeatable)
- `--env KEY=VALUE` (repeatable)
- `--dry-run`
- `--verbose / --no-verbose`
- `--show-config`
- `--version`

Use `--` to force all remaining arguments to be forwarded:

```bash
kub-simulate --runtime docker -- --help
```

## Image model

Canonical upstream reference is Docker/OCI.

Example Docker image:

```text
ghcr.io/feelpp/ktirio-urban-building:master
```

Derived Apptainer remote source:

```text
oras://ghcr.io/feelpp/ktirio-urban-building:master-sif

Default image references used when no explicit image is configured:

- Docker: `ghcr.io/feelpp/ktirio-urban-building:master`
- Apptainer: `oras://ghcr.io/feelpp/ktirio-urban-building:master-sif`

Other tags are supported (for example `pr-<nnn>`), e.g.:

- Docker: `ghcr.io/feelpp/ktirio-urban-building:pr-456`
- Apptainer source: `oras://ghcr.io/feelpp/ktirio-urban-building:pr-456-sif`
```

Important:

- For Apptainer download/pull, use `oras://...`.
- Do not use `docker://...` for Apptainer pulls in this workflow.

## kub-img

`kub-img` is the image utility command used by `kub-cli` internals for image pull/info workflows.

Subcommands:

- `kub-img pull [SOURCE] [--runtime ...] [--image ...]`
- `kub-img info [--runtime ...] [--image ...] [--json]`
- `kub-img apps` (Apptainer runtime)
- `kub-img path`

Examples:

```bash
# Docker pull
kub-img pull --runtime docker --image ghcr.io/feelpp/ktirio-urban-building:master

# Apptainer pull with ORAS source
kub-img pull oras://ghcr.io/feelpp/ktirio-urban-building:master-sif \
  --runtime apptainer \
  --image ./ktirio-urban-building.sif

# Runtime-aware image info
kub-img info --runtime docker --image ghcr.io/feelpp/ktirio-urban-building:master --json
kub-img info --runtime apptainer --image ./ktirio-urban-building.sif
```

## Configuration

Precedence (highest to lowest):

1. CLI options
2. Environment variables
3. Project config: `.kub-cli.toml` in current working directory
4. User config: `~/.config/kub-cli/config.toml`
5. Built-in defaults

### Environment variables

- `KUB_RUNTIME` : `auto|apptainer|docker`
- `KUB_IMAGE_DOCKER` : Docker image reference
- `KUB_IMAGE_APPTAINER` : Apptainer image path (local SIF)
- `KUB_IMAGE` : legacy generic image fallback (backward compatibility)
- `KUB_BIND` : additional binds, comma- or semicolon-separated
- `KUB_WORKDIR` : runtime working directory
- `KUB_APP_RUNNER` : generic runner override
- `KUB_APPTAINER_RUNNER` : Apptainer runner override
- `KUB_DOCKER_RUNNER` : Docker runner override
- `KUB_VERBOSE` : boolean (`true/false`, `1/0`, `yes/no`, `on/off`)
- `KUB_APPTAINER_FLAGS` : extra Apptainer flags (shell-split)
- `KUB_DOCKER_FLAGS` : extra Docker flags (shell-split)

### Config file format

You can place config at:

- `~/.config/kub-cli/config.toml`
- `.kub-cli.toml`

Keys may be top-level or under `[kub_cli]`.

```toml
[kub_cli]
runtime = "auto"
workdir = "/work"
verbose = false

[kub_cli.image]
docker = "ghcr.io/feelpp/ktirio-urban-building:master"
apptainer = "./ktirio-urban-building.sif"

[kub_cli.env]
OMP_NUM_THREADS = "8"
```

## Backward compatibility

`kub-cli` keeps compatibility with existing Apptainer-centric usage:

- Existing `KUB_IMAGE` still works as a fallback image setting.
- Existing Apptainer command flow remains unchanged when runtime resolves to `apptainer`.
- Existing wrapper UX and argument forwarding semantics are preserved.

For explicit multi-runtime setups, prefer `KUB_IMAGE_DOCKER` and `KUB_IMAGE_APPTAINER`.

## Troubleshooting

- Docker runtime selected but Docker missing: install Docker or set `--runner`.
- Apptainer runtime selected but Apptainer missing: install Apptainer or set `--runner`.
- No image configured for selected runtime: set `--image`, runtime-specific env vars, or config.
- Need to inspect resolved command: use `--dry-run`.

## Development

```bash
uv venv .venv
. .venv/bin/activate
uv pip install -e '.[dev]'
pytest
```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
