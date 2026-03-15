# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""High-level command orchestration for kub-cli wrappers."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .config import KubConfig, KubConfigOverrides, loadKubConfig
from .errors import ConfigError
from .img_integration import (
    KubImgCommandRunner,
    buildKubImgInfoRequest,
    buildKubImgPullRequest,
)
from .logging_utils import configureLogging
from .runtime import KubAppRunner


CEMDB_CONTAINER_ROOT = "/cemdb"
CEMDB_OPTION = "--cemdb-root"
HOME_ENV = "HOME"
HOME_CONTAINER_ROOT = CEMDB_CONTAINER_ROOT
KUB_CONFIG_ENV = "KUB_CONFIG"
KUB_CONFIG_CONTAINER_PATH = "/cemdb/.kub/config.toml"


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

    hasUserForwardedArgs = bool(forwardedArgs)
    preparedOptions, preparedForwardedArgs = prepareCemdbContext(
        options=options,
        forwardedArgs=forwardedArgs,
        cwd=cwd,
    )

    effectiveConfig = resolveEffectiveConfig(
        options=preparedOptions,
        cwd=cwd,
        env=env,
        userConfigPath=userConfigPath,
    )

    configureLogging(effectiveConfig.verbose)

    if options.showConfig:
        print(json.dumps(effectiveConfig.toDict(), indent=2, sort_keys=True))
        if not hasUserForwardedArgs and not options.dryRun:
            return 0

    runner = KubAppRunner(config=effectiveConfig)
    return runner.run(
        appName=appName,
        forwardedArgs=preparedForwardedArgs,
        dryRun=options.dryRun,
    )


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


def prepareCemdbContext(
    *,
    options: WrapperOptions,
    forwardedArgs: Sequence[str],
    cwd: Path | None,
) -> tuple[WrapperOptions, list[str]]:
    runtimeCwd = (cwd or Path.cwd()).resolve()
    rewrittenArgs, forwardedCemdbRoot = rewriteForwardedCemdbArgs(forwardedArgs)

    if options.cemdbRoot is not None:
        selectedHostCemdb = options.cemdbRoot
    elif forwardedCemdbRoot is not None:
        selectedHostCemdb = forwardedCemdbRoot
    else:
        selectedHostCemdb = str(runtimeCwd)

    hostCemdbRoot = resolveCemdbHostRoot(selectedHostCemdb, cwd=runtimeCwd)
    ensureHostKubConfigDirectory(Path(hostCemdbRoot))

    updatedBinds = list(options.binds)
    if not hasCemdbBind(updatedBinds):
        updatedBinds.append(f"{hostCemdbRoot}:{CEMDB_CONTAINER_ROOT}")

    updatedEnvVars = list(options.envVars)
    ensureEnvAssignment(updatedEnvVars, HOME_ENV, HOME_CONTAINER_ROOT)
    ensureEnvAssignment(updatedEnvVars, KUB_CONFIG_ENV, KUB_CONFIG_CONTAINER_PATH)

    resolvedWorkdir = options.pwd if options.pwd is not None else CEMDB_CONTAINER_ROOT

    return replace(
        options,
        binds=tuple(updatedBinds),
        envVars=tuple(updatedEnvVars),
        pwd=resolvedWorkdir,
    ), rewrittenArgs


def rewriteForwardedCemdbArgs(forwardedArgs: Sequence[str]) -> tuple[list[str], str | None]:
    rewritten: list[str] = []
    hostCemdbRoot: str | None = None
    rawArgs = list(forwardedArgs)
    index = 0

    while index < len(rawArgs):
        token = rawArgs[index]

        if token == CEMDB_OPTION:
            if index + 1 >= len(rawArgs):
                raise ConfigError(
                    f"Missing value for forwarded {CEMDB_OPTION}. "
                    "Provide a host directory path."
                )

            rawValue = rawArgs[index + 1]
            if hostCemdbRoot is None:
                hostCemdbRoot = rawValue

            rewritten.extend([CEMDB_OPTION, CEMDB_CONTAINER_ROOT])
            index += 2
            continue

        if token.startswith(f"{CEMDB_OPTION}="):
            rawValue = token.split("=", maxsplit=1)[1]
            if not rawValue.strip():
                raise ConfigError(
                    f"Empty value for forwarded {CEMDB_OPTION}=... option."
                )

            if hostCemdbRoot is None:
                hostCemdbRoot = rawValue

            rewritten.append(f"{CEMDB_OPTION}={CEMDB_CONTAINER_ROOT}")
            index += 1
            continue

        rewritten.append(token)
        index += 1

    return rewritten, hostCemdbRoot


def resolveCemdbHostRoot(rawValue: str, *, cwd: Path) -> str:
    normalized = rawValue.strip()
    if not normalized:
        raise ConfigError("CEMDB root path cannot be empty.")

    pathValue = Path(normalized).expanduser()
    if not pathValue.is_absolute():
        pathValue = (cwd / pathValue).resolve()
    else:
        pathValue = pathValue.resolve()

    if not pathValue.exists():
        try:
            pathValue.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ConfigError(
                f"Unable to create CEMDB root path '{pathValue}': {error}"
            ) from error

    if not pathValue.is_dir():
        raise ConfigError(f"CEMDB root path must be a directory: '{pathValue}'.")

    return str(pathValue)


def hasCemdbBind(bindSpecs: Sequence[str]) -> bool:
    for bindSpec in bindSpecs:
        parts = bindSpec.split(":")
        if len(parts) < 2:
            continue
        if parts[1] == CEMDB_CONTAINER_ROOT:
            return True
    return False


def ensureHostKubConfigDirectory(hostCemdbRoot: Path) -> None:
    configDir = hostCemdbRoot / ".kub"
    try:
        configDir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigError(
            f"Unable to create CEMDB config directory '{configDir}': {error}"
        ) from error


def ensureEnvAssignment(assignments: list[str], key: str, value: str) -> None:
    if hasEnvAssignment(assignments, key):
        return
    assignments.append(f"{key}={value}")


def hasEnvAssignment(assignments: Sequence[str], key: str) -> bool:
    for entry in assignments:
        if "=" not in entry:
            continue
        entryKey = entry.split("=", maxsplit=1)[0].strip()
        if entryKey == key:
            return True
    return False
