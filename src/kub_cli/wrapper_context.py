# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Runtime/CEMDB wrapper context preparation utilities."""

from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import shutil
from typing import TYPE_CHECKING, Sequence

from .app_policy import BaseAppPolicy
from .config import KubConfig
from .errors import ConfigError


if TYPE_CHECKING:
    from .commands import WrapperOptions


CEMDB_CONTAINER_ROOT = "/cemdb"
CEMDB_OPTION = "--cemdb-root"
HOME_ENV = "HOME"
HOME_CONTAINER_ROOT = CEMDB_CONTAINER_ROOT
KUB_CONFIG_ENV = "KUB_CONFIG"
KUB_CONFIG_CONTAINER_PATH = "/cemdb/.kub/config.toml"
SIMULATE_HOST_CONFIG_FILENAME = ".kub-simulate.toml"
SLURM_SHIMS_CONTAINER_DIR = "/cemdb/.kub-cli/shims"
SLURM_HOST_BRIDGE_CONTAINER_DIR = "/cemdb/.kub-cli/host-bin"
SLURM_SHIM_COMMANDS = ("sbatch", "srun")
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


def prepareCemdbContext(
    *,
    options: WrapperOptions,
    policy: BaseAppPolicy,
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
    rewrittenArgs = policy.rewriteForwardedArgs(rewrittenArgs)
    ensureHostKubConfigDirectory(Path(hostCemdbRoot))

    updatedBinds = list(options.binds)
    useHostPathContext = policy.shouldUseHostPathContext(rewrittenArgs)
    if useHostPathContext:
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
    if policy.shouldSyncConfigProjection():
        if ensureHostSlurmBridge(hostCemdbRoot=Path(hostCemdbRoot)):
            prependPathEnvAssignment(updatedEnvVars, SLURM_HOST_BRIDGE_CONTAINER_DIR)
            exposeHostSlurmSupportFiles(bindSpecs=updatedBinds)
        elif policy.shouldAddCompatibilityShims(rewrittenArgs):
            ensureSlurmCompatibilityShims(hostCemdbRoot=Path(hostCemdbRoot))
            prependPathEnvAssignment(updatedEnvVars, SLURM_SHIMS_CONTAINER_DIR)
        if policy.shouldExposeInnerRuntimeExecutable(rewrittenArgs):
            ensureInnerApptainerExecutableVisibility(
                bindSpecs=updatedBinds,
                envAssignments=updatedEnvVars,
                hostCemdbRoot=Path(hostCemdbRoot),
                configHint=configHint,
            )

    if options.pwd is not None:
        resolvedWorkdir = options.pwd
    elif useHostPathContext:
        resolvedWorkdir = str(runtimeCwd)
    else:
        resolvedWorkdir = CEMDB_CONTAINER_ROOT

    return replace(
        options,
        binds=tuple(updatedBinds),
        envVars=tuple(updatedEnvVars),
        pwd=resolvedWorkdir,
    ), rewrittenArgs, hostCemdbRoot


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
