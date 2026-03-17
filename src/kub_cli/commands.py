# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""High-level command orchestration for kub-cli wrappers."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import shutil
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
SIMULATE_APP_NAME = "kub-simulate"
SIMULATE_CONFIG_OPTION = "--config"
SIMULATE_CONFIG_CONTAINER_PATH = "/cemdb/.kub-simulate.toml"
SIMULATE_HOST_CONFIG_FILENAME = ".kub-simulate.toml"
SLURM_SHIMS_CONTAINER_DIR = "/cemdb/.kub-cli/shims"
SLURM_HOST_BRIDGE_CONTAINER_DIR = "/cemdb/.kub-cli/host-bin"
SLURM_SHIM_COMMANDS = ("sbatch", "srun")
SIMULATE_PREPROCESS_COMMAND = "preprocess"
DEFAULT_CONTAINER_PATH = (
    "/opt/kub-venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)
SLURM_LIBRARY_DIR_CANDIDATES = (
    Path("/usr/lib/x86_64-linux-gnu/slurm-wlm"),
    Path("/usr/lib64/slurm"),
    Path("/usr/lib/slurm"),
)
SLURM_CONFIG_DIR_CANDIDATES = (
    Path("/etc/slurm"),
    Path("/etc/slurm-llnl"),
)
SLURM_IDENTITY_FILE_CANDIDATES = (
    Path("/etc/passwd"),
    Path("/etc/group"),
    Path("/etc/nsswitch.conf"),
)
SLURM_MUNGE_PATH_CANDIDATES = (
    Path("/run/munge"),
    Path("/var/run/munge"),
    Path("/etc/munge"),
)


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

    hasExplicitSimulateConfig = (
        appName == SIMULATE_APP_NAME
        and hasForwardedOption(forwardedArgs, SIMULATE_CONFIG_OPTION)
    )

    hasUserForwardedArgs = bool(forwardedArgs)
    preparedOptions, preparedForwardedArgs, hostCemdbRoot = prepareCemdbContext(
        appName=appName,
        options=options,
        forwardedArgs=forwardedArgs,
        cwd=cwd,
        configHint=initialConfig,
    )

    if appName == SIMULATE_APP_NAME and not hasExplicitSimulateConfig:
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

    if options.showConfig:
        print(json.dumps(effectiveConfig.toDict(), indent=2, sort_keys=True))
        if not hasUserForwardedArgs and not options.dryRun:
            return 0

    runner = KubAppRunner(config=effectiveConfig)
    exitCode = runner.run(
        appName=appName,
        forwardedArgs=preparedForwardedArgs,
        dryRun=options.dryRun,
    )

    if appName == SIMULATE_APP_NAME and not hasExplicitSimulateConfig:
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


def prepareCemdbContext(
    *,
    appName: str,
    options: WrapperOptions,
    forwardedArgs: Sequence[str],
    cwd: Path | None,
    configHint: KubConfig | None = None,
) -> tuple[WrapperOptions, list[str], str]:
    runtimeCwd = (cwd or Path.cwd()).resolve()
    rewrittenArgs, forwardedCemdbRoot = rewriteForwardedCemdbArgs(forwardedArgs)

    if options.cemdbRoot is not None:
        selectedHostCemdb = options.cemdbRoot
    elif forwardedCemdbRoot is not None:
        selectedHostCemdb = forwardedCemdbRoot
    else:
        selectedHostCemdb = str(runtimeCwd)

    hostCemdbRoot = resolveCemdbHostRoot(selectedHostCemdb, cwd=runtimeCwd)
    rewrittenArgs = withDefaultSimulateConfig(
        appName=appName,
        forwardedArgs=rewrittenArgs,
    )
    ensureHostKubConfigDirectory(Path(hostCemdbRoot))

    updatedBinds = list(options.binds)
    useHostPathSlurmContext = shouldUseHostPathForSimulateSlurm(
        appName=appName,
        forwardedArgs=rewrittenArgs,
    )
    if useHostPathSlurmContext:
        addBindIfMissing(
            updatedBinds,
            source=str(runtimeCwd),
            destination=str(runtimeCwd),
        )
    if not hasCemdbBind(updatedBinds):
        updatedBinds.append(f"{hostCemdbRoot}:{CEMDB_CONTAINER_ROOT}")

    updatedEnvVars = list(options.envVars)
    ensureEnvAssignment(updatedEnvVars, HOME_ENV, HOME_CONTAINER_ROOT)
    ensureEnvAssignment(updatedEnvVars, KUB_CONFIG_ENV, KUB_CONFIG_CONTAINER_PATH)
    if appName == SIMULATE_APP_NAME:
        if ensureHostSlurmBridge(hostCemdbRoot=Path(hostCemdbRoot)):
            prependPathEnvAssignment(updatedEnvVars, SLURM_HOST_BRIDGE_CONTAINER_DIR)
            exposeHostSlurmSupportFiles(bindSpecs=updatedBinds)
        elif shouldAddSlurmCompatibilityShims(
            appName=appName,
            forwardedArgs=rewrittenArgs,
        ):
            ensureSlurmCompatibilityShims(hostCemdbRoot=Path(hostCemdbRoot))
            prependPathEnvAssignment(updatedEnvVars, SLURM_SHIMS_CONTAINER_DIR)
        if shouldExposeInnerApptainerExecutable(
            appName=appName,
            forwardedArgs=rewrittenArgs,
        ):
            ensureInnerApptainerExecutableVisibility(
                bindSpecs=updatedBinds,
                envAssignments=updatedEnvVars,
                hostCemdbRoot=Path(hostCemdbRoot),
                configHint=configHint,
            )

    if options.pwd is not None:
        resolvedWorkdir = options.pwd
    elif useHostPathSlurmContext:
        resolvedWorkdir = str(runtimeCwd)
    else:
        resolvedWorkdir = CEMDB_CONTAINER_ROOT

    return replace(
        options,
        binds=tuple(updatedBinds),
        envVars=tuple(updatedEnvVars),
        pwd=resolvedWorkdir,
    ), rewrittenArgs, hostCemdbRoot


def withDefaultSimulateConfig(
    *,
    appName: str,
    forwardedArgs: list[str],
) -> list[str]:
    if appName != SIMULATE_APP_NAME:
        return forwardedArgs

    if hasForwardedOption(forwardedArgs, SIMULATE_CONFIG_OPTION):
        return forwardedArgs

    return [
        SIMULATE_CONFIG_OPTION,
        SIMULATE_CONFIG_CONTAINER_PATH,
        *forwardedArgs,
    ]


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


def hasForwardedOption(forwardedArgs: Sequence[str], optionName: str) -> bool:
    return any(
        token == optionName or token.startswith(f"{optionName}=")
        for token in forwardedArgs
    )


def shouldAddSlurmCompatibilityShims(
    *,
    appName: str,
    forwardedArgs: Sequence[str],
) -> bool:
    if appName != SIMULATE_APP_NAME:
        return False

    if hasForwardedOption(forwardedArgs, "--dry-run"):
        return True

    return detectSimulateSubcommand(forwardedArgs) == SIMULATE_PREPROCESS_COMMAND


def detectSimulateSubcommand(forwardedArgs: Sequence[str]) -> str | None:
    for token in forwardedArgs:
        if token == SIMULATE_PREPROCESS_COMMAND:
            return token
    return None


def shouldExposeInnerApptainerExecutable(
    *,
    appName: str,
    forwardedArgs: Sequence[str],
) -> bool:
    if appName != SIMULATE_APP_NAME:
        return False

    runtimeValue = getForwardedOptionValue(forwardedArgs, "--runtime")
    if runtimeValue is not None and runtimeValue.strip().lower() == "apptainer":
        return True

    profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
    return profileValue is not None and "apptainer" in profileValue.lower()


def shouldUseHostPathForSimulateSlurm(
    *,
    appName: str,
    forwardedArgs: Sequence[str],
) -> bool:
    if appName != SIMULATE_APP_NAME:
        return False

    launcherValue = getForwardedOptionValue(forwardedArgs, "--launcher")
    if launcherValue is not None and launcherValue.strip().lower() == "slurm":
        return True

    profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
    return profileValue is not None and "slurm" in profileValue.lower()


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


def ensureInnerApptainerExecutableVisibility(
    *,
    bindSpecs: list[str],
    envAssignments: list[str],
    hostCemdbRoot: Path,
    configHint: KubConfig | None,
) -> None:
    executablePath = resolveHostApptainerExecutablePath(configHint=configHint)
    if executablePath is None:
        ensureApptainerCompatibilityShim(hostCemdbRoot=hostCemdbRoot)
        prependPathEnvAssignment(envAssignments, SLURM_SHIMS_CONTAINER_DIR)
        return

    executableDir = executablePath.parent
    addBindIfMissing(
        bindSpecs,
        source=str(executableDir),
        destination=str(executableDir),
    )
    prependPathEnvAssignment(envAssignments, str(executableDir))


def resolveHostApptainerExecutablePath(*, configHint: KubConfig | None) -> Path | None:
    candidates: list[str] = []
    if configHint is not None:
        if configHint.runner is not None and configHint.runner.strip():
            candidates.append(configHint.runner.strip())
        if configHint.apptainerRunner.strip():
            candidates.append(configHint.apptainerRunner.strip())

    discovered = findExecutable("apptainer")
    if discovered is not None:
        candidates.append(discovered)

    for value in candidates:
        candidatePath = Path(value).expanduser()
        if not candidatePath.is_absolute():
            continue
        if candidatePath.exists() and os.access(candidatePath, os.X_OK):
            return candidatePath.resolve()

    return None


def ensureApptainerCompatibilityShim(*, hostCemdbRoot: Path) -> None:
    shimDir = hostCemdbRoot / ".kub-cli" / "shims"
    try:
        shimDir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigError(
            f"Unable to create shim directory '{shimDir}': {error}"
        ) from error

    shimPath = shimDir / "apptainer"
    shimContent = (
        "#!/bin/sh\n"
        "echo \"kub-cli shim: apptainer executable not available in container PATH.\" >&2\n"
        "echo \"Install/bind Apptainer or set --runtime native in kub-simulate profile.\" >&2\n"
        "exit 127\n"
    )
    try:
        shimPath.write_text(shimContent, encoding="utf-8")
        shimPath.chmod(0o755)
    except OSError as error:
        raise ConfigError(
            f"Unable to create apptainer shim '{shimPath}': {error}"
        ) from error


def addBindIfMissing(bindSpecs: list[str], *, source: str, destination: str) -> None:
    for bindSpec in bindSpecs:
        parts = bindSpec.split(":")
        if len(parts) < 2:
            continue
        if parts[0] == source and parts[1] == destination:
            return
    bindSpecs.append(f"{source}:{destination}")


def exposeHostSlurmSupportFiles(*, bindSpecs: list[str]) -> None:
    for directory in SLURM_LIBRARY_DIR_CANDIDATES:
        if directory.is_dir():
            addBindIfMissing(
                bindSpecs,
                source=str(directory),
                destination=str(directory),
            )

    for directory in SLURM_CONFIG_DIR_CANDIDATES:
        if directory.is_dir():
            addBindIfMissing(
                bindSpecs,
                source=str(directory),
                destination=str(directory),
            )

    for filePath in SLURM_IDENTITY_FILE_CANDIDATES:
        if filePath.is_file():
            addBindIfMissing(
                bindSpecs,
                source=str(filePath),
                destination=str(filePath),
            )

    for pathValue in SLURM_MUNGE_PATH_CANDIDATES:
        if pathValue.is_dir():
            addBindIfMissing(
                bindSpecs,
                source=str(pathValue),
                destination=str(pathValue),
            )


def ensureHostSlurmBridge(*, hostCemdbRoot: Path) -> bool:
    hostCommands: dict[str, Path] = {}
    for commandName in SLURM_SHIM_COMMANDS:
        resolved = findExecutable(commandName)
        if resolved is None:
            return False
        sourcePath = Path(resolved)
        if (
            sourcePath.name != commandName
            or not sourcePath.exists()
            or not os.access(sourcePath, os.X_OK)
        ):
            return False
        hostCommands[commandName] = sourcePath

    hostBinDir = hostCemdbRoot / ".kub-cli" / "host-bin"
    try:
        hostBinDir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigError(
            f"Unable to create host slurm bridge directory '{hostBinDir}': {error}"
        ) from error

    for commandName, sourcePath in hostCommands.items():
        destination = hostBinDir / commandName
        if destination.exists():
            try:
                if destination.samefile(sourcePath):
                    destination.chmod(0o755)
                    continue
            except OSError:
                pass
        try:
            shutil.copy2(sourcePath, destination)
            destination.chmod(0o755)
        except OSError as error:
            raise ConfigError(
                f"Unable to prepare host slurm bridge command '{destination}': {error}"
            ) from error

    return True


def findExecutable(commandName: str) -> str | None:
    return shutil.which(commandName)


def ensureSlurmCompatibilityShims(*, hostCemdbRoot: Path) -> None:
    shimDir = hostCemdbRoot / ".kub-cli" / "shims"
    try:
        shimDir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigError(
            f"Unable to create slurm shim directory '{shimDir}': {error}"
        ) from error

    shimContent = "#!/bin/sh\nexit 0\n"
    for commandName in SLURM_SHIM_COMMANDS:
        shimPath = shimDir / commandName
        try:
            shimPath.write_text(shimContent, encoding="utf-8")
            shimPath.chmod(0o755)
        except OSError as error:
            raise ConfigError(
                f"Unable to create slurm shim '{shimPath}': {error}"
            ) from error


def prependPathEnvAssignment(assignments: list[str], prefixPath: str) -> None:
    existingPath = getEnvAssignmentValue(assignments, "PATH")
    if existingPath is not None:
        if existingPath == prefixPath or existingPath.startswith(f"{prefixPath}:"):
            return
        setEnvAssignmentValue(assignments, "PATH", f"{prefixPath}:{existingPath}")
        return

    setEnvAssignmentValue(assignments, "PATH", f"{prefixPath}:{DEFAULT_CONTAINER_PATH}")


def getEnvAssignmentValue(assignments: Sequence[str], key: str) -> str | None:
    for entry in assignments:
        if "=" not in entry:
            continue
        entryKey, value = entry.split("=", maxsplit=1)
        if entryKey.strip() == key:
            return value
    return None


def setEnvAssignmentValue(assignments: list[str], key: str, value: str) -> None:
    for index, entry in enumerate(assignments):
        if "=" not in entry:
            continue
        entryKey = entry.split("=", maxsplit=1)[0].strip()
        if entryKey == key:
            assignments[index] = f"{key}={value}"
            return
    assignments.append(f"{key}={value}")


def syncSimulateConfigProjection(*, hostCemdbRoot: Path, mirrorToNested: bool) -> None:
    rootConfig = hostCemdbRoot / SIMULATE_HOST_CONFIG_FILENAME
    nestedDir = hostCemdbRoot / "cemdb"
    nestedConfig = nestedDir / SIMULATE_HOST_CONFIG_FILENAME

    if not nestedDir.is_dir():
        return

    if not rootConfig.exists() and nestedConfig.exists() and not nestedConfig.is_dir():
        try:
            shutil.copy2(nestedConfig, rootConfig)
        except OSError as error:
            raise ConfigError(
                f"Unable to initialize {rootConfig} from existing nested config: {error}"
            ) from error

    if not mirrorToNested:
        return

    if not rootConfig.exists():
        return

    if nestedConfig.is_symlink():
        target = os.readlink(nestedConfig)
        if target == f"../{SIMULATE_HOST_CONFIG_FILENAME}":
            return
        try:
            nestedConfig.unlink()
        except OSError as error:
            raise ConfigError(
                f"Unable to update nested kub-simulate config symlink '{nestedConfig}': {error}"
            ) from error

    if not nestedConfig.exists():
        try:
            nestedConfig.symlink_to(Path("..") / SIMULATE_HOST_CONFIG_FILENAME)
            return
        except OSError:
            pass

    if nestedConfig.is_dir():
        raise ConfigError(
            f"Nested kub-simulate config path is a directory: '{nestedConfig}'."
        )

    try:
        shutil.copy2(rootConfig, nestedConfig)
    except OSError as error:
        raise ConfigError(
            f"Unable to mirror kub-simulate config to '{nestedConfig}': {error}"
        ) from error


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
