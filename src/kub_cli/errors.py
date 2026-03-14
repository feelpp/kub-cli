# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""kub-cli exception hierarchy."""

from __future__ import annotations


class KubCliError(RuntimeError):
    """Base class for user-facing CLI errors."""

    def __init__(self, message: str, *, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class ConfigError(KubCliError):
    """Raised when configuration cannot be parsed or validated."""


class RunnerNotFoundError(KubCliError):
    """Raised when the Apptainer executable cannot be located."""


class ImageNotFoundError(KubCliError):
    """Raised when the image path is missing or invalid."""


class RuntimeSelectionError(KubCliError):
    """Raised when the configured runtime cannot be resolved."""
