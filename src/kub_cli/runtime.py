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
from typing import Literal, Mapping, Sequence

from .config import (
    DEFAULT_APPTAINER_IMAGE,
    DEFAULT_DOCKER_IMAGE,
    KubConfig,
    SUPPORTED_RUNTIMES,
)
from .errors import KubCliError, RunnerNotFoundError, RuntimeSelectionError
from .image_resolution import resolveApptainerExecutionImage, resolveDockerExecutionImage
from . import image_resolution as imageResolution
from .logging_utils import LOGGER, formatCommand


ResolvedRuntime = Literal["apptainer", "docker"]
DASHBOARD_APP_NAME = "kub-dashboard"


@dataclass(frozen=True)
class RuntimeResolution:
    """Concrete runtime and image selected for command execution."""

    runtime: ResolvedRuntime
    runnerPath: str
    imageReference: str


def deriveApptainerOrasReference(dockerImageReference: str) -> str:
    """Backward-compatible export for ORAS derivation."""

    return imageResolution.deriveApptainerOrasReference(dockerImageReference)


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
            try:
                imageReference = resolveDockerExecutionImage(
                    config,
                    strictImageOverride=False,
                    strictLegacyImage=False,
                )
                return RuntimeResolution(
                    runtime="docker",
                    runnerPath=dockerRunner,
                    imageReference=imageReference,
                )
            except KubCliError as error:
                diagnostics.append(f"Docker not selected: {error}")
        else:
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

    def buildExec(self, forwardedArgs: Sequence[str]) -> list[str]:
        runner = self.resolveRunner()
        image = self.resolveImage()

        command: list[str] = [runner, "exec"]

        if self.config.apptainerFlags:
            command.extend(self.config.apptainerFlags)

        for bindSpec in self.config.binds:
            command.extend(["--bind", bindSpec])

        if self.config.workdir:
            command.extend(["--pwd", self.config.workdir])

        command.extend([image, self.appName])
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

    def build(
        self,
        forwardedArgs: Sequence[str],
        *,
        imageReference: str | None = None,
    ) -> list[str]:
        runner = self.resolveRunner()
        resolvedImageReference = imageReference or self.resolveImage()

        command: list[str] = [runner, "run", "--rm"]

        if self.config.dockerFlags:
            command.extend(self.config.dockerFlags)

        if self.appName == DASHBOARD_APP_NAME and not dockerFlagsContainNetwork(
            self.config.dockerFlags
        ):
            command.extend(["--network", "host"])

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

        command.append(resolvedImageReference)
        command.append(self.appName)
        command.extend(forwardedArgs)

        return command


def dockerFlagsContainUser(dockerFlags: Sequence[str]) -> bool:
    return any(
        flag == "--user" or flag.startswith("--user=")
        for flag in dockerFlags
    )


def dockerFlagsContainNetwork(dockerFlags: Sequence[str]) -> bool:
    return any(
        flag in {"--network", "--net"}
        or flag.startswith("--network=")
        or flag.startswith("--net=")
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
            command = builder.build(forwardedArgs)
            if not dryRun:
                try:
                    if shouldUseApptainerExecForLocalImage(
                        runnerPath=runtimeResolution.runnerPath,
                        imageReference=runtimeResolution.imageReference,
                        appName=appName,
                    ):
                        command = builder.buildExec(forwardedArgs)
                        if self.config.verbose:
                            LOGGER.debug(
                                "Apptainer app '%s' not found in local image; using exec fallback.",
                                appName,
                            )
                except KeyboardInterrupt:
                    return 130
        else:
            builder = DockerCommandBuilder(appName=appName, config=self.config)
            command = builder.build(
                forwardedArgs,
                imageReference=runtimeResolution.imageReference,
            )

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
            injectApptainerContainerEnv(executionEnv, self.config.env)

        try:
            completed = subprocess.run(command, check=False, env=executionEnv)
        except KeyboardInterrupt:
            return 130
        except OSError as error:
            raise KubCliError(f"Unable to execute runtime command: {error}") from error

        return completed.returncode


def injectApptainerContainerEnv(
    executionEnv: dict[str, str],
    containerEnv: Sequence[tuple[str, str]] | Mapping[str, str],
) -> None:
    """Propagate explicit container env through Apptainer runtime variables."""

    if isinstance(containerEnv, Mapping):
        entries = containerEnv.items()
    else:
        entries = containerEnv

    disallowedKeys = {"HOME"}

    for key, value in entries:
        if key in disallowedKeys:
            continue
        executionEnv[f"APPTAINERENV_{key}"] = value
        executionEnv[f"SINGULARITYENV_{key}"] = value


def shouldUseApptainerExecForLocalImage(
    *,
    runnerPath: str,
    imageReference: str,
    appName: str,
) -> bool:
    if "://" in imageReference:
        return False

    imagePath = Path(imageReference).expanduser()
    if not imagePath.exists() or imagePath.is_dir():
        return False

    apps = inspectApptainerApps(
        runnerPath=runnerPath,
        imagePath=imagePath,
    )
    if apps is None:
        return False

    return appName not in apps


def inspectApptainerApps(
    *,
    runnerPath: str,
    imagePath: Path,
) -> set[str] | None:
    inspectCommand = [
        runnerPath,
        "inspect",
        "--list-apps",
        str(imagePath),
    ]

    try:
        completed = subprocess.run(
            inspectCommand,
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, TypeError):
        return None

    if completed.returncode != 0:
        return None

    return {
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip()
    }
