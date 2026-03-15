# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Container runtime resolution, command construction, and process execution."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import subprocess
from typing import Literal, Sequence

from .config import (
    DEFAULT_APPTAINER_IMAGE,
    DEFAULT_DOCKER_IMAGE,
    KubConfig,
    SUPPORTED_RUNTIMES,
    looksLikeContainerReference,
)
from .errors import ImageNotFoundError, KubCliError, RunnerNotFoundError, RuntimeSelectionError
from .logging_utils import LOGGER, formatCommand


ResolvedRuntime = Literal["apptainer", "docker"]


@dataclass(frozen=True)
class RuntimeResolution:
    """Concrete runtime and image selected for command execution."""

    runtime: ResolvedRuntime
    runnerPath: str
    imageReference: str


def deriveApptainerOrasReference(dockerImageReference: str) -> str:
    """Derive Apptainer ORAS source from a Docker image reference.

    Example:
    `ghcr.io/org/app:master` -> `oras://ghcr.io/org/app:master-sif`
    """

    normalized = dockerImageReference.strip()
    if not normalized:
        raise KubCliError("Docker image reference cannot be empty.")

    if "@" in normalized:
        raise KubCliError(
            "Cannot derive Apptainer ORAS reference from digest-based Docker image. "
            "Provide a tag-based Docker image reference."
        )

    if normalized.startswith("oras://"):
        return normalized

    if "://" in normalized:
        raise KubCliError(
            "Docker image reference for ORAS derivation must not include a URI scheme. "
            "Use format like ghcr.io/org/image:tag"
        )

    lastSlash = normalized.rfind("/")
    lastColon = normalized.rfind(":")

    if lastColon > lastSlash:
        repository = normalized[:lastColon]
        tag = normalized[lastColon + 1 :]
    else:
        repository = normalized
        tag = "latest"

    if not repository or not tag:
        raise KubCliError(
            "Invalid Docker image reference for ORAS derivation. "
            "Expected format like ghcr.io/org/image:tag"
        )

    return f"oras://{repository}:{tag}-sif"


def getRuntimeCandidateImage(config: KubConfig, runtime: ResolvedRuntime) -> str | None:
    """Resolve configured image by runtime-specific precedence."""

    if runtime == "docker":
        candidates = [
            config.imageOverride,
            config.imageDocker,
            config.image,
            DEFAULT_DOCKER_IMAGE,
        ]
    else:
        candidates = [
            config.imageOverride,
            config.imageApptainer,
            config.image,
            DEFAULT_APPTAINER_IMAGE,
        ]

    for candidate in candidates:
        if candidate is None:
            continue
        normalized = candidate.strip()
        if normalized:
            return normalized

    return None


def getRunnerValue(config: KubConfig, runtime: ResolvedRuntime) -> str:
    """Resolve configured runner name/path by runtime."""

    if config.runner is not None and config.runner.strip():
        return config.runner.strip()

    if runtime == "apptainer":
        return config.apptainerRunner.strip()

    return config.dockerRunner.strip()


def resolveRunnerExecutable(runnerValue: str, *, runtimeName: str) -> str:
    """Resolve runner executable path and validate it is runnable."""

    normalized = runnerValue.strip()
    if not normalized:
        raise RunnerNotFoundError(
            f"{runtimeName.capitalize()} runner is empty. Set --runner or runtime-specific runner config."
        )

    runnerPath = Path(normalized).expanduser()
    hasPathSeparator = runnerPath.parent != Path(".")

    if runnerPath.is_absolute() or hasPathSeparator:
        if runnerPath.exists() and os.access(runnerPath, os.X_OK):
            return str(runnerPath)

        raise RunnerNotFoundError(
            f"{runtimeName.capitalize()} runner not executable: '{runnerPath}'."
        )

    resolvedRunner = shutil.which(normalized)
    if resolvedRunner is None:
        if runtimeName == "apptainer":
            installHint = (
                "Install Apptainer: "
                "https://apptainer.org/docs/admin/main/installation.html"
            )
        elif runtimeName == "docker":
            installHint = (
                "Install Docker Engine: "
                "https://docs.docker.com/engine/install/"
            )
        else:
            installHint = "Install the selected runtime executable."

        raise RunnerNotFoundError(
            f"Unable to find {runtimeName} runner in PATH. "
            f"Set --runner/KUB_APP_RUNNER or install it. {installHint}"
        )

    return resolvedRunner


def tryResolveRunnerExecutable(runnerValue: str) -> str | None:
    """Try to resolve runner executable, returning None on failure."""

    normalized = runnerValue.strip()
    if not normalized:
        return None

    runnerPath = Path(normalized).expanduser()
    hasPathSeparator = runnerPath.parent != Path(".")

    if runnerPath.is_absolute() or hasPathSeparator:
        if runnerPath.exists() and os.access(runnerPath, os.X_OK):
            return str(runnerPath)
        return None

    return shutil.which(normalized)


def resolveApptainerExecutionImage(config: KubConfig) -> str:
    """Resolve Apptainer execution image reference (local path or oras:// URI)."""

    imageReference = getRuntimeCandidateImage(config, "apptainer")
    if imageReference is None:
        raise ImageNotFoundError(
            "No Apptainer image configured for runtime 'apptainer'. "
            "Set --image, KUB_IMAGE_APPTAINER, or KUB_IMAGE."
        )

    normalizedReference = imageReference.strip()

    if normalizedReference.startswith("docker://"):
        raise ImageNotFoundError(
            "Apptainer image reference must use oras:// (or a local .sif path), "
            "not docker://."
        )

    if normalizedReference.startswith("oras://"):
        return normalizedReference

    if "://" in normalizedReference:
        raise ImageNotFoundError(
            "Unsupported Apptainer image URI scheme. "
            "Use oras://<registry>/<image>:<tag>-sif or a local .sif path."
        )

    if looksLikeContainerReference(normalizedReference):
        return f"oras://{normalizedReference}"

    imagePath = Path(normalizedReference).expanduser()

    if not imagePath.exists():
        raise ImageNotFoundError(f"Container image not found: '{imagePath}'.")

    if imagePath.is_dir():
        raise ImageNotFoundError(
            f"Container image must be a file, got directory: '{imagePath}'."
        )

    return str(imagePath)


def resolveDockerExecutionImage(config: KubConfig) -> str:
    """Resolve Docker image reference used at runtime execution."""

    imageReference = getRuntimeCandidateImage(config, "docker")
    if imageReference is None:
        raise ImageNotFoundError(
            "No Docker image configured for runtime 'docker'. "
            "Set --image, KUB_IMAGE_DOCKER, or KUB_IMAGE."
        )

    return imageReference


def resolveRuntimeForExecution(config: KubConfig) -> RuntimeResolution:
    """Resolve runtime backend, executable, and image for application execution."""

    configuredRuntime = config.runtime.strip().lower()
    if configuredRuntime not in SUPPORTED_RUNTIMES:
        supported = ", ".join(sorted(SUPPORTED_RUNTIMES))
        raise RuntimeSelectionError(
            f"Invalid runtime value '{config.runtime}'. Use one of: {supported}."
        )

    if configuredRuntime == "apptainer":
        return resolveApptainerRuntime(config)

    if configuredRuntime == "docker":
        return resolveDockerRuntime(config)

    return resolveAutoRuntime(config)


def resolveApptainerRuntime(config: KubConfig) -> RuntimeResolution:
    runnerValue = getRunnerValue(config, "apptainer")
    runnerPath = resolveRunnerExecutable(runnerValue, runtimeName="apptainer")
    imageReference = resolveApptainerExecutionImage(config)
    return RuntimeResolution(
        runtime="apptainer",
        runnerPath=runnerPath,
        imageReference=imageReference,
    )


def resolveDockerRuntime(config: KubConfig) -> RuntimeResolution:
    runnerValue = getRunnerValue(config, "docker")
    runnerPath = resolveRunnerExecutable(runnerValue, runtimeName="docker")
    imageReference = resolveDockerExecutionImage(config)
    return RuntimeResolution(
        runtime="docker",
        runnerPath=runnerPath,
        imageReference=imageReference,
    )


def resolveAutoRuntime(config: KubConfig) -> RuntimeResolution:
    """Resolve runtime in auto mode using preference order.

    Policy:
    1. Apptainer if configured and available.
    2. Docker if configured and available.
    3. Fail with actionable diagnostics.
    """

    diagnostics: list[str] = []

    apptainerImage = getRuntimeCandidateImage(config, "apptainer")
    if apptainerImage is not None:
        apptainerRunnerValue = getRunnerValue(config, "apptainer")
        apptainerRunner = tryResolveRunnerExecutable(apptainerRunnerValue)
        if apptainerRunner is not None:
            try:
                imageReference = resolveApptainerExecutionImage(config)
                return RuntimeResolution(
                    runtime="apptainer",
                    runnerPath=apptainerRunner,
                    imageReference=imageReference,
                )
            except KubCliError as error:
                diagnostics.append(f"Apptainer not selected: {error}")
        else:
            diagnostics.append(
                "Apptainer not selected: runner not available in PATH or not executable."
            )
    else:
        diagnostics.append(
            "Apptainer not selected: no Apptainer image configured."
        )

    dockerImage = getRuntimeCandidateImage(config, "docker")
    if dockerImage is not None:
        dockerRunnerValue = getRunnerValue(config, "docker")
        dockerRunner = tryResolveRunnerExecutable(dockerRunnerValue)
        if dockerRunner is not None:
            return RuntimeResolution(
                runtime="docker",
                runnerPath=dockerRunner,
                imageReference=dockerImage,
            )
        diagnostics.append(
            "Docker not selected: runner not available in PATH or not executable."
        )
    else:
        diagnostics.append(
            "Docker not selected: no Docker image configured."
        )

    diagnosticText = " ".join(diagnostics)
    raise RuntimeSelectionError(
        "Unable to resolve runtime in auto mode. "
        "Configure a valid runtime/image pair or set --runtime explicitly. "
        "Install Apptainer (https://apptainer.org/docs/admin/main/installation.html) "
        "or Docker Engine (https://docs.docker.com/engine/install/). "
        f"Details: {diagnosticText}"
    )


@dataclass(frozen=True)
class ApptainerCommandBuilder:
    """Build final `apptainer run` command lines for wrapped apps."""

    appName: str
    config: KubConfig

    def resolveRunner(self) -> str:
        runnerValue = getRunnerValue(self.config, "apptainer")
        return resolveRunnerExecutable(runnerValue, runtimeName="apptainer")

    def resolveImage(self) -> str:
        return resolveApptainerExecutionImage(self.config)

    def build(self, forwardedArgs: Sequence[str]) -> list[str]:
        runner = self.resolveRunner()
        image = self.resolveImage()

        command: list[str] = [runner, "run"]

        if self.config.apptainerFlags:
            command.extend(self.config.apptainerFlags)

        for bindSpec in self.config.binds:
            command.extend(["--bind", bindSpec])

        if self.config.workdir:
            command.extend(["--pwd", self.config.workdir])

        command.extend(["--app", self.appName, image])
        command.extend(forwardedArgs)

        return command


@dataclass(frozen=True)
class DockerCommandBuilder:
    """Build final `docker run` command lines for wrapped apps."""

    appName: str
    config: KubConfig

    def resolveRunner(self) -> str:
        runnerValue = getRunnerValue(self.config, "docker")
        return resolveRunnerExecutable(runnerValue, runtimeName="docker")

    def resolveImage(self) -> str:
        return resolveDockerExecutionImage(self.config)

    def build(self, forwardedArgs: Sequence[str]) -> list[str]:
        runner = self.resolveRunner()
        imageReference = self.resolveImage()

        command: list[str] = [runner, "run", "--rm"]

        if self.config.dockerFlags:
            command.extend(self.config.dockerFlags)

        if not dockerFlagsContainUser(self.config.dockerFlags):
            userValue = buildDockerUserValue()
            if userValue is not None:
                command.extend(["--user", userValue])

        for bindSpec in self.config.binds:
            command.extend(["--volume", bindSpec])

        if self.config.workdir:
            command.extend(["--workdir", self.config.workdir])

        for key, value in self.config.env.items():
            command.extend(["--env", f"{key}={value}"])

        command.append(imageReference)
        command.append(self.appName)
        command.extend(forwardedArgs)

        return command


def dockerFlagsContainUser(dockerFlags: Sequence[str]) -> bool:
    return any(
        flag == "--user" or flag.startswith("--user=")
        for flag in dockerFlags
    )


def buildDockerUserValue() -> str | None:
    if not hasattr(os, "getuid") or not hasattr(os, "getgid"):
        return None

    return f"{os.getuid()}:{os.getgid()}"


@dataclass(frozen=True)
class KubAppRunner:
    """Execute wrapped containerized apps using resolved configuration."""

    config: KubConfig

    def run(
        self,
        *,
        appName: str,
        forwardedArgs: Sequence[str],
        dryRun: bool = False,
    ) -> int:
        runtimeResolution = resolveRuntimeForExecution(self.config)

        if runtimeResolution.runtime == "apptainer":
            builder = ApptainerCommandBuilder(appName=appName, config=self.config)
        else:
            builder = DockerCommandBuilder(appName=appName, config=self.config)

        command = builder.build(forwardedArgs)

        if self.config.verbose:
            LOGGER.debug(
                "Resolved runtime=%s command: %s",
                runtimeResolution.runtime,
                formatCommand(command),
            )

        if dryRun:
            print(formatCommand(command))
            return 0

        executionEnv = dict(os.environ)
        if runtimeResolution.runtime == "apptainer":
            executionEnv.update(self.config.env)

        try:
            completed = subprocess.run(command, check=False, env=executionEnv)
        except KeyboardInterrupt:
            return 130
        except OSError as error:
            raise KubCliError(f"Unable to execute runtime command: {error}") from error

        return completed.returncode
