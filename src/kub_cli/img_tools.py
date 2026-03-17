# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Image management helpers for the kub-img command."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Mapping, Sequence

from .config import KubConfig, KubConfigOverrides, SUPPORTED_RUNTIMES, loadKubConfig
from .errors import ImageNotFoundError, KubCliError
from .img_integration import (
    buildKubImgInfoRequest,
    buildKubImgPullRequest,
)
from .logging_utils import LOGGER, formatCommand
from .runtime import getRunnerValue, resolveRunnerExecutable


@dataclass(frozen=True)
class KubImgManager:
    """Execute runtime-specific image operations using resolved kub-cli config."""

    config: KubConfig

    def resolveRuntime(self, runtime: str | None) -> str:
        if runtime is None:
            return self.config.runtime

        normalized = runtime.strip().lower()
        if normalized not in SUPPORTED_RUNTIMES:
            supported = ", ".join(sorted(SUPPORTED_RUNTIMES))
            raise KubCliError(
                f"Invalid runtime value '{runtime}'. Use one of: {supported}."
            )

        return normalized

    def configWithRuntime(self, runtime: str | None) -> KubConfig:
        resolvedRuntime = self.resolveRuntime(runtime)
        return replace(self.config, runtime=resolvedRuntime)

    def pullImage(
        self,
        *,
        runtime: str | None,
        source: str | None,
        force: bool,
        disableCache: bool,
        apptainerFlags: Sequence[str],
        dockerFlags: Sequence[str],
        dryRun: bool,
    ) -> int:
        runtimeConfig = self.configWithRuntime(runtime)
        request = buildKubImgPullRequest(runtimeConfig)

        if source is not None:
            normalizedSource = source.strip()
            if not normalizedSource:
                raise KubCliError("Image source cannot be empty.")
            request = replace(request, source=normalizedSource)

        if request.runtime == "apptainer":
            return self.pullApptainerImage(
                runtimeConfig=runtimeConfig,
                source=request.source,
                destinationImage=request.image,
                force=force,
                disableCache=disableCache,
                apptainerFlags=apptainerFlags,
                dryRun=dryRun,
            )

        return self.pullDockerImage(
            runtimeConfig=runtimeConfig,
            source=request.source,
            destinationImage=request.image,
            dockerFlags=dockerFlags,
            dryRun=dryRun,
        )

    def pullApptainerImage(
        self,
        *,
        runtimeConfig: KubConfig,
        source: str,
        destinationImage: str,
        force: bool,
        disableCache: bool,
        apptainerFlags: Sequence[str],
        dryRun: bool,
    ) -> int:
        if source.startswith("docker://"):
            raise KubCliError(
                "Apptainer pull source must use oras:// and not docker://."
            )

        runner = self.resolveRunner("apptainer", runtimeConfig)

        command: list[str] = [runner, "pull"]

        if force:
            command.append("--force")

        if disableCache:
            command.append("--disable-cache")

        if runtimeConfig.apptainerFlags:
            command.extend(runtimeConfig.apptainerFlags)

        if apptainerFlags:
            command.extend(apptainerFlags)

        command.extend([destinationImage, source])

        return self.runCommand(command, captureOutput=False, dryRun=dryRun)

    def pullDockerImage(
        self,
        *,
        runtimeConfig: KubConfig,
        source: str,
        destinationImage: str,
        dockerFlags: Sequence[str],
        dryRun: bool,
    ) -> int:
        runner = self.resolveRunner("docker", runtimeConfig)

        pullCommand: list[str] = [runner, "pull"]
        if runtimeConfig.dockerFlags:
            pullCommand.extend(runtimeConfig.dockerFlags)
        if dockerFlags:
            pullCommand.extend(dockerFlags)
        pullCommand.append(source)

        exitCode = self.runCommand(pullCommand, captureOutput=False, dryRun=dryRun)
        if exitCode != 0:
            return exitCode

        if destinationImage == source:
            return 0

        tagCommand = [runner, "tag", source, destinationImage]
        return self.runCommand(tagCommand, captureOutput=False, dryRun=dryRun)

    def collectInfo(self, *, runtime: str | None) -> dict[str, Any]:
        runtimeConfig = self.configWithRuntime(runtime)
        request = buildKubImgInfoRequest(runtimeConfig)

        if request.runtime == "apptainer":
            return self.collectApptainerInfo(request.image, runtimeConfig)

        return self.collectDockerInfo(request.image, runtimeConfig)

    def collectApptainerInfo(self, imagePathRaw: str, runtimeConfig: KubConfig) -> dict[str, Any]:
        imagePath = Path(imagePathRaw).expanduser()
        if not imagePath.exists():
            raise ImageNotFoundError(f"Container image not found: '{imagePath}'.")

        if imagePath.is_dir():
            raise ImageNotFoundError(
                f"Container image must be a file, got directory: '{imagePath}'."
            )

        apps = self.inspectApptainerApps(imagePath, runtimeConfig)
        labelsRaw = self.inspectApptainerLabels(imagePath, runtimeConfig)

        imageStat = imagePath.stat()

        return {
            "runtime": "apptainer",
            "image": str(imagePath),
            "sizeBytes": imageStat.st_size,
            "modifiedUtc": datetime.fromtimestamp(
                imageStat.st_mtime,
                tz=timezone.utc,
            ).isoformat(),
            "apps": apps,
            "labels": parseLabelOutput(labelsRaw),
            "labelsRaw": labelsRaw,
        }

    def collectDockerInfo(self, imageReference: str, runtimeConfig: KubConfig) -> dict[str, Any]:
        runner = self.resolveRunner("docker", runtimeConfig)
        command = [runner, "image", "inspect", imageReference]
        completed = self.runCommand(
            command,
            captureOutput=True,
            dryRun=False,
            runtimeConfig=runtimeConfig,
        )

        if not isinstance(completed, subprocess.CompletedProcess):
            raise KubCliError("Internal error while collecting Docker image information.")

        if completed.returncode != 0:
            stderrText = completed.stderr.strip() if completed.stderr else ""
            raise KubCliError(
                "Unable to inspect Docker image. "
                f"Command failed with code {completed.returncode}. {stderrText}".strip()
            )

        payload: Any
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise KubCliError("Docker inspect did not return valid JSON output.") from error

        return {
            "runtime": "docker",
            "image": imageReference,
            "inspect": payload,
        }

    def inspectApptainerApps(self, imagePath: Path, runtimeConfig: KubConfig) -> list[str]:
        runner = self.resolveRunner("apptainer", runtimeConfig)
        command = [runner, "inspect", "--list-apps", str(imagePath)]
        completed = self.runCommand(
            command,
            captureOutput=True,
            dryRun=False,
            runtimeConfig=runtimeConfig,
        )

        if not isinstance(completed, subprocess.CompletedProcess):
            raise KubCliError("Internal error while listing Apptainer apps.")

        if completed.returncode != 0:
            stderrText = completed.stderr.strip() if completed.stderr else ""
            raise KubCliError(
                "Unable to list Apptainer apps from image. "
                f"Command failed with code {completed.returncode}. {stderrText}".strip()
            )

        return [line.strip() for line in completed.stdout.splitlines() if line.strip()]

    def inspectApptainerLabels(self, imagePath: Path, runtimeConfig: KubConfig) -> str:
        runner = self.resolveRunner("apptainer", runtimeConfig)
        command = [runner, "inspect", "--labels", str(imagePath)]
        completed = self.runCommand(
            command,
            captureOutput=True,
            dryRun=False,
            runtimeConfig=runtimeConfig,
        )

        if not isinstance(completed, subprocess.CompletedProcess):
            raise KubCliError("Internal error while inspecting Apptainer labels.")

        if completed.returncode != 0:
            stderrText = completed.stderr.strip() if completed.stderr else ""
            raise KubCliError(
                "Unable to inspect Apptainer image labels. "
                f"Command failed with code {completed.returncode}. {stderrText}".strip()
            )

        return completed.stdout.strip()

    def printInfo(self, *, runtime: str | None, jsonOutput: bool) -> int:
        info = self.collectInfo(runtime=runtime)

        if jsonOutput:
            print(json.dumps(info, indent=2, sort_keys=True))
            return 0

        print(f"Runtime: {info['runtime']}")
        print(f"Image: {info['image']}")

        if info["runtime"] == "apptainer":
            print(f"Size: {info['sizeBytes']} bytes")
            print(f"Modified (UTC): {info['modifiedUtc']}")

            apps: list[str] = info["apps"]
            if apps:
                print("Apps:")
                for app in apps:
                    print(f"  - {app}")
            else:
                print("Apps: (none listed)")

            labels: Mapping[str, str] = info["labels"]
            if labels:
                print("Labels:")
                for key in sorted(labels.keys()):
                    print(f"  {key}: {labels[key]}")
            else:
                labelsRaw: str = info["labelsRaw"]
                if labelsRaw:
                    print("Labels (raw):")
                    print(labelsRaw)
                else:
                    print("Labels: (none)")
        else:
            inspectData = info["inspect"]
            if isinstance(inspectData, list) and inspectData:
                first = inspectData[0]
                if isinstance(first, Mapping):
                    repoTags = first.get("RepoTags")
                    if repoTags is not None:
                        print(f"RepoTags: {repoTags}")
                    imageId = first.get("Id")
                    if imageId is not None:
                        print(f"Id: {imageId}")

        return 0

    def printApps(self, *, runtime: str | None) -> int:
        runtimeConfig = self.configWithRuntime(runtime)
        request = buildKubImgInfoRequest(runtimeConfig)

        if request.runtime != "apptainer":
            raise KubCliError(
                "kub-img apps is only available with Apptainer runtime. "
                "Use --runtime apptainer."
            )

        imagePathRaw = request.image
        imagePath = Path(imagePathRaw).expanduser()
        if not imagePath.exists():
            raise ImageNotFoundError(f"Container image not found: '{imagePath}'.")

        apps = self.inspectApptainerApps(imagePath, runtimeConfig)

        for app in apps:
            print(app)

        return 0

    def printImagePath(self, *, runtime: str | None) -> int:
        runtimeConfig = self.configWithRuntime(runtime)
        request = buildKubImgInfoRequest(runtimeConfig)
        print(request.image)
        return 0

    def resolveRunner(self, runtime: str, runtimeConfig: KubConfig) -> str:
        runnerValue = getRunnerValue(runtimeConfig, runtime)  # type: ignore[arg-type]
        return resolveRunnerExecutable(runnerValue, runtimeName=runtime)

    def runCommand(
        self,
        command: Sequence[str],
        *,
        captureOutput: bool,
        dryRun: bool,
        runtimeConfig: KubConfig | None = None,
    ) -> int | subprocess.CompletedProcess[str]:
        if self.config.verbose or (runtimeConfig is not None and runtimeConfig.verbose):
            LOGGER.debug("Resolved command: %s", formatCommand(command))

        if dryRun:
            print(formatCommand(command))
            return 0

        if captureOutput:
            runKwargs: dict[str, Any] = {
                "capture_output": True,
                "text": True,
            }
        else:
            runKwargs = {}

        try:
            return subprocess.run(
                list(command),
                check=False,
                env=dict(os.environ),
                **runKwargs,
            )
        except KeyboardInterrupt as error:
            raise KubCliError("Execution interrupted by user.", exit_code=130) from error
        except OSError as error:
            raise KubCliError(f"Unable to execute runtime command: {error}") from error


def resolveImgConfig(
    *,
    runtime: str | None,
    image: str | None,
    runner: str | None,
    verbose: bool | None,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    userConfigPath: Path | None = None,
) -> KubConfig:
    overrides = KubConfigOverrides(
        runtime=runtime,
        image=image,
        runner=runner,
        verbose=verbose,
    )
    return loadKubConfig(
        cwd=cwd,
        env=env,
        overrides=overrides,
        userConfigPath=userConfigPath,
    )


def parseLabelOutput(rawText: str) -> dict[str, str]:
    labels: dict[str, str] = {}

    for line in rawText.splitlines():
        normalized = line.strip()
        if not normalized or ":" not in normalized:
            continue

        key, value = normalized.split(":", maxsplit=1)
        labels[key.strip()] = value.strip()

    return labels
