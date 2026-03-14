# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Internal helpers to invoke the kub-img utility via subprocess."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any, Literal

from .config import KubConfig, SUPPORTED_RUNTIMES
from .errors import KubCliError, RuntimeSelectionError
from .logging_utils import LOGGER, formatCommand
from .runtime import (
    deriveApptainerOrasReference,
    getRuntimeCandidateImage,
    getRunnerValue,
    tryResolveRunnerExecutable,
)


ImageRuntime = Literal["apptainer", "docker"]


@dataclass(frozen=True)
class KubImgPullRequest:
    """Resolved arguments for a kub-img pull operation."""

    runtime: ImageRuntime
    image: str
    source: str


@dataclass(frozen=True)
class KubImgInfoRequest:
    """Resolved arguments for a kub-img info operation."""

    runtime: ImageRuntime
    image: str


@dataclass(frozen=True)
class KubImgCommandRunner:
    """Invoke `kub-img` as a subprocess safely."""

    executable: str = "kub-img"
    verbose: bool = False

    def resolveExecutable(self) -> str:
        executablePath = Path(self.executable).expanduser()
        hasPathSeparator = executablePath.parent != Path(".")

        if executablePath.is_absolute() or hasPathSeparator:
            if executablePath.exists() and os.access(executablePath, os.X_OK):
                return str(executablePath)
            raise KubCliError(f"kub-img executable not found or not executable: '{executablePath}'")

        resolved = shutil.which(self.executable)
        if resolved is None:
            raise KubCliError(
                "kub-img executable not found in PATH. Install kub-cli with the kub-img entrypoint."
            )

        return resolved

    def pullImage(self, request: KubImgPullRequest, *, dryRun: bool = False) -> int:
        executable = self.resolveExecutable()
        command = [
            executable,
            "pull",
            "--runtime",
            request.runtime,
            "--image",
            request.image,
            request.source,
        ]

        if self.verbose:
            command.append("--verbose")
            LOGGER.debug("Running kub-img pull command: %s", formatCommand(command))

        if dryRun:
            print(formatCommand(command))
            return 0

        try:
            completed = subprocess.run(command, check=False)
        except OSError as error:
            raise KubCliError(f"Unable to execute kub-img pull command: {error}") from error

        if completed.returncode != 0:
            raise KubCliError(
                f"kub-img pull failed with exit code {completed.returncode}.",
                exit_code=completed.returncode,
            )

        return 0

    def inspectImageInfo(self, request: KubImgInfoRequest) -> dict[str, Any]:
        executable = self.resolveExecutable()
        command = [
            executable,
            "info",
            "--runtime",
            request.runtime,
            "--image",
            request.image,
            "--json",
        ]

        if self.verbose:
            command.append("--verbose")
            LOGGER.debug("Running kub-img info command: %s", formatCommand(command))

        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as error:
            raise KubCliError(f"Unable to execute kub-img info command: {error}") from error

        if completed.returncode != 0:
            stderrText = completed.stderr.strip()
            message = (
                f"kub-img info failed with exit code {completed.returncode}. "
                f"{stderrText}".strip()
            )
            raise KubCliError(message, exit_code=completed.returncode)

        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise KubCliError(
                "kub-img info returned non-JSON output while --json was requested."
            ) from error

        if not isinstance(payload, dict):
            raise KubCliError("kub-img info JSON output must be an object.")

        return payload


def resolveImageRuntime(config: KubConfig) -> ImageRuntime:
    """Resolve runtime for image pull/info operations."""

    configured = config.runtime.strip().lower()
    if configured not in SUPPORTED_RUNTIMES:
        supported = ", ".join(sorted(SUPPORTED_RUNTIMES))
        raise RuntimeSelectionError(
            f"Invalid runtime value '{config.runtime}'. Use one of: {supported}."
        )

    if configured in {"apptainer", "docker"}:
        return configured

    apptainerRunnerValue = getRunnerValue(config, "apptainer")
    apptainerRunner = tryResolveRunnerExecutable(apptainerRunnerValue)
    if apptainerRunner is not None:
        try:
            resolveApptainerLocalImageReference(config)
            return "apptainer"
        except KubCliError:
            pass

    dockerRunnerValue = getRunnerValue(config, "docker")
    dockerRunner = tryResolveRunnerExecutable(dockerRunnerValue)
    dockerImage = getRuntimeCandidateImage(config, "docker")
    if dockerRunner is not None and dockerImage is not None:
        return "docker"

    raise RuntimeSelectionError(
        "Unable to resolve runtime in auto mode for image operations. "
        "Neither Apptainer nor Docker runner is available."
    )


def buildKubImgInfoRequest(config: KubConfig) -> KubImgInfoRequest:
    runtime = resolveImageRuntime(config)

    if runtime == "docker":
        dockerImage = getRuntimeCandidateImage(config, "docker")
        if dockerImage is None:
            raise KubCliError(
                "No Docker image configured for image info. "
                "Set --image, KUB_IMAGE_DOCKER, or KUB_IMAGE."
            )
        return KubImgInfoRequest(runtime="docker", image=dockerImage)

    apptainerImage = resolveApptainerLocalImageReference(config)
    return KubImgInfoRequest(runtime="apptainer", image=apptainerImage)


def buildKubImgPullRequest(config: KubConfig) -> KubImgPullRequest:
    runtime = resolveImageRuntime(config)

    if runtime == "docker":
        dockerImage = getRuntimeCandidateImage(config, "docker")
        if dockerImage is None:
            raise KubCliError(
                "No Docker image configured for pull. "
                "Set --image, KUB_IMAGE_DOCKER, or KUB_IMAGE."
            )

        return KubImgPullRequest(
            runtime="docker",
            image=dockerImage,
            source=dockerImage,
        )

    destinationImage = resolveApptainerLocalImageReference(config)

    explicitApptainerReference = config.imageApptainer
    if explicitApptainerReference is not None and explicitApptainerReference.startswith("oras://"):
        source = explicitApptainerReference
    else:
        dockerImage = getRuntimeCandidateImage(config, "docker")
        if dockerImage is None:
            raise KubCliError(
                "Unable to derive Apptainer ORAS source: no Docker image is configured. "
                "Set KUB_IMAGE_DOCKER (or runtime image config)."
            )
        source = deriveApptainerOrasReference(dockerImage)

    if source.startswith("docker://"):
        raise KubCliError(
            "Apptainer image pull source must use oras://, not docker://."
        )

    return KubImgPullRequest(
        runtime="apptainer",
        image=destinationImage,
        source=source,
    )


def resolveApptainerLocalImageReference(config: KubConfig) -> str:
    candidates = [config.imageOverride, config.imageApptainer, config.image]

    for candidate in candidates:
        if candidate is None:
            continue

        normalized = candidate.strip()
        if not normalized:
            continue

        if "://" in normalized:
            continue

        return normalized

    raise KubCliError(
        "No local Apptainer image path configured. "
        "Set --image or KUB_IMAGE_APPTAINER to a .sif file path."
    )
