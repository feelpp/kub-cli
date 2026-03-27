# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""High-level command orchestration for kub-cli wrappers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .app_policy import getAppPolicy
from .config import KubConfig, KubConfigOverrides, loadKubConfig
from .errors import ConfigError
from .img_integration import (
    KubImgCommandRunner,
    buildKubImgInfoRequest,
    buildKubImgPullRequest,
)
from .logging_utils import configureLogging
from .preflight import runWrapperPreflight
from .runtime import KubAppRunner
from .wrapper_context import prepareCemdbContext, syncSimulateConfigProjection


@dataclass(frozen=True)
class WrapperOptions:
    """Wrapper-specific options parsed from the CLI layer."""

    runtime: str | None = None
    image: str | None = None
    binds: tuple[str, ...] = ()
    pwd: str | None = None
    runner: str | None = None
    dryRun: bool = False
    verbose: bool | None = None
    apptainerFlags: tuple[str, ...] = ()
    dockerFlags: tuple[str, ...] = ()
    envVars: tuple[str, ...] = ()
    cemdbRoot: str | None = None
    showConfig: bool = False


def runWrapperCommand(
    *,
    appName: str,
    forwardedArgs: Sequence[str],
    options: WrapperOptions,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    userConfigPath: Path | None = None,
) -> int:
    """Resolve config and execute one wrapped in-container app."""

    initialConfig = resolveEffectiveConfig(
        options=options,
        cwd=cwd,
        env=env,
        userConfigPath=userConfigPath,
    )
    configureLogging(initialConfig.verbose)

    if options.showConfig:
        print(json.dumps(initialConfig.toDict(), indent=2, sort_keys=True))
        return 0

    runWrapperPreflight(
        appName=appName,
        forwardedArgs=forwardedArgs,
        config=initialConfig,
    )

    policy = getAppPolicy(appName)
    hasExplicitPolicyConfig = policy.hasExplicitWrapperConfig(forwardedArgs)

    preparedOptions, preparedForwardedArgs, hostCemdbRoot = prepareCemdbContext(
        options=options,
        policy=policy,
        forwardedArgs=forwardedArgs,
        cwd=cwd,
        configHint=initialConfig,
    )

    if policy.shouldSyncConfigProjection() and not hasExplicitPolicyConfig:
        syncSimulateConfigProjection(
            hostCemdbRoot=Path(hostCemdbRoot),
            mirrorToNested=False,
        )

    effectiveConfig = resolveEffectiveConfig(
        options=preparedOptions,
        cwd=cwd,
        env=env,
        userConfigPath=userConfigPath,
    )

    configureLogging(effectiveConfig.verbose)

    runner = KubAppRunner(config=effectiveConfig)
    exitCode = runner.run(
        appName=appName,
        forwardedArgs=preparedForwardedArgs,
        dryRun=options.dryRun,
    )

    if policy.shouldSyncConfigProjection() and not hasExplicitPolicyConfig:
        syncSimulateConfigProjection(
            hostCemdbRoot=Path(hostCemdbRoot),
            mirrorToNested=True,
        )

    return exitCode


def pullSelectedRuntimeImage(
    *,
    options: WrapperOptions,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    userConfigPath: Path | None = None,
) -> int:
    """Invoke kub-img pull for the runtime/image selected by config precedence."""

    effectiveConfig = resolveEffectiveConfig(
        options=options,
        cwd=cwd,
        env=env,
        userConfigPath=userConfigPath,
    )
    configureLogging(effectiveConfig.verbose)

    request = buildKubImgPullRequest(effectiveConfig)
    kubImgRunner = KubImgCommandRunner(verbose=effectiveConfig.verbose)
    return kubImgRunner.pullImage(request, dryRun=options.dryRun)


def inspectSelectedRuntimeImage(
    *,
    options: WrapperOptions,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    userConfigPath: Path | None = None,
) -> dict[str, Any]:
    """Invoke kub-img info for the runtime/image selected by config precedence."""

    effectiveConfig = resolveEffectiveConfig(
        options=options,
        cwd=cwd,
        env=env,
        userConfigPath=userConfigPath,
    )
    configureLogging(effectiveConfig.verbose)

    request = buildKubImgInfoRequest(effectiveConfig)
    kubImgRunner = KubImgCommandRunner(verbose=effectiveConfig.verbose)
    return kubImgRunner.inspectImageInfo(request)


def resolveEffectiveConfig(
    *,
    options: WrapperOptions,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    userConfigPath: Path | None = None,
) -> KubConfig:
    """Build the effective config from defaults, files, env, then CLI overrides."""

    overrideEnv = parseEnvAssignments(options.envVars)
    overrides = KubConfigOverrides(
        runtime=options.runtime,
        image=options.image,
        binds=options.binds,
        workdir=options.pwd,
        runner=options.runner,
        verbose=options.verbose,
        apptainerFlags=options.apptainerFlags,
        dockerFlags=options.dockerFlags,
        env=overrideEnv,
    )

    return loadKubConfig(
        cwd=cwd,
        env=env,
        overrides=overrides,
        userConfigPath=userConfigPath,
    )


def parseEnvAssignments(assignments: Sequence[str]) -> dict[str, str]:
    envMapping: dict[str, str] = {}

    for entry in assignments:
        if "=" not in entry:
            raise ConfigError(
                f"Invalid --env assignment '{entry}'. Expected KEY=VALUE syntax."
            )

        key, value = entry.split("=", maxsplit=1)
        normalizedKey = key.strip()
        if not normalizedKey:
            raise ConfigError(
                f"Invalid --env assignment '{entry}'. Environment key cannot be empty."
            )

        envMapping[normalizedKey] = value

    return envMapping
