# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
from pathlib import Path
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(autouse=True)
def disableRunnerProbeByDefault(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep existing unit tests focused unless they explicitly exercise probing."""

    monkeypatch.setattr(
        "kub_cli.runtime.probeRunnerExecutable",
        lambda runnerPath, *, runtimeName: None,
    )
