# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Logging helpers for kub-cli."""

from __future__ import annotations

import logging
import shlex
from typing import Sequence


LOGGER = logging.getLogger("kub_cli")


def configureLogging(verbose: bool) -> None:
    """Configure process-wide logging according to verbosity."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def formatCommand(arguments: Sequence[str]) -> str:
    """Format a subprocess argument list as a shell-style string."""
    return shlex.join([str(item) for item in arguments])
