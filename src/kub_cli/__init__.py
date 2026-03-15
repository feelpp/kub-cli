# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""kub-cli package."""

from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("kub-cli")
except PackageNotFoundError:
    __version__ = "0.2.0"


__all__ = ["__version__"]
