# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""App policy abstractions for wrapper orchestration behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


SIMULATE_APP_NAME = "kub-simulate"
SIMULATE_CONFIG_OPTION = "--config"
SIMULATE_CONFIG_CONTAINER_PATH = "/cemdb/.kub-simulate.toml"
SIMULATE_PREPROCESS_COMMAND = "preprocess"


@dataclass(frozen=True)
class BaseAppPolicy:
    """Default app policy with no special forwarding or runtime behavior."""

    appName: str

    def hasExplicitWrapperConfig(self, forwardedArgs: Sequence[str]) -> bool:
        return False

    def rewriteForwardedArgs(self, forwardedArgs: Sequence[str]) -> list[str]:
        return list(forwardedArgs)

    def shouldSyncConfigProjection(self) -> bool:
        return False

    def shouldUseHostPathContext(self, forwardedArgs: Sequence[str]) -> bool:
        return False

    def shouldAddCompatibilityShims(self, forwardedArgs: Sequence[str]) -> bool:
        return False

    def shouldExposeInnerRuntimeExecutable(self, forwardedArgs: Sequence[str]) -> bool:
        return False


@dataclass(frozen=True)
class SimulateAppPolicy(BaseAppPolicy):
    """Policy for kub-simulate wrapper-specific argument/runtime behavior."""

    def hasExplicitWrapperConfig(self, forwardedArgs: Sequence[str]) -> bool:
        return hasForwardedOption(forwardedArgs, SIMULATE_CONFIG_OPTION)

    def rewriteForwardedArgs(self, forwardedArgs: Sequence[str]) -> list[str]:
        rewritten = list(forwardedArgs)
        if self.hasExplicitWrapperConfig(rewritten):
            return rewritten

        return [
            SIMULATE_CONFIG_OPTION,
            SIMULATE_CONFIG_CONTAINER_PATH,
            *rewritten,
        ]

    def shouldSyncConfigProjection(self) -> bool:
        return True

    def shouldUseHostPathContext(self, forwardedArgs: Sequence[str]) -> bool:
        launcherValue = getForwardedOptionValue(forwardedArgs, "--launcher")
        if launcherValue is not None and launcherValue.strip().lower() == "slurm":
            return True

        profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
        return profileValue is not None and "slurm" in profileValue.lower()

    def shouldAddCompatibilityShims(self, forwardedArgs: Sequence[str]) -> bool:
        if hasForwardedOption(forwardedArgs, "--dry-run"):
            return True

        return detectSimulateSubcommand(forwardedArgs) == SIMULATE_PREPROCESS_COMMAND

    def shouldExposeInnerRuntimeExecutable(self, forwardedArgs: Sequence[str]) -> bool:
        runtimeValue = getForwardedOptionValue(forwardedArgs, "--runtime")
        if runtimeValue is not None and runtimeValue.strip().lower() == "apptainer":
            return True

        profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
        return profileValue is not None and "apptainer" in profileValue.lower()


def getAppPolicy(appName: str) -> BaseAppPolicy:
    if appName == SIMULATE_APP_NAME:
        return SimulateAppPolicy(appName=appName)
    return BaseAppPolicy(appName=appName)


def hasForwardedOption(forwardedArgs: Sequence[str], optionName: str) -> bool:
    return any(
        token == optionName or token.startswith(f"{optionName}=")
        for token in forwardedArgs
    )


def detectSimulateSubcommand(forwardedArgs: Sequence[str]) -> str | None:
    for token in forwardedArgs:
        if token == SIMULATE_PREPROCESS_COMMAND:
            return token
    return None


def getForwardedOptionValue(forwardedArgs: Sequence[str], optionName: str) -> str | None:
    rawArgs = list(forwardedArgs)
    for index, token in enumerate(rawArgs):
        if token == optionName:
            if index + 1 < len(rawArgs):
                return rawArgs[index + 1]
            return None
        if token.startswith(f"{optionName}="):
            return token.split("=", maxsplit=1)[1]
    return None
