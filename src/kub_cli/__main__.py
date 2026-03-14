# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Module entrypoint for `python -m kub_cli`."""

from .cli import metaMain


if __name__ == "__main__":
    metaMain()
