#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    # In a Git checkout, lint tracked and non-ignored untracked files.
    # uv.lock is generated and intentionally excluded from REUSE lint.
    git ls-files --cached --others --exclude-standard -z -- ':!uv.lock' | xargs -0 -r reuse lint-file
else
    # Fallback for archive/local directories without VCS metadata.
    find . \
        -type d \( \
            -name .git -o \
            -name .venv -o \
            -name venv -o \
            -name __pycache__ -o \
            -name .pytest_cache -o \
            -name .mypy_cache -o \
            -name .ruff_cache \
        \) -prune -o \
        -name uv.lock -prune -o \
        -type f -print0 | xargs -0 -r reuse lint-file
fi
