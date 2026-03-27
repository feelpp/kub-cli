# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Runtime/CEMDB wrapper context preparation utilities."""

from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
from typing import TYPE_CHECKING, Sequence

from .app_policy import BaseAppPolicy, getForwardedOptionValue
from .config import KubConfig
from .errors import ConfigError
from .runtime import tryResolveRunnerExecutable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python < 3.11
    import tomli as tomllib  # type: ignore[import-not-found]


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
SSSD_RUNTIME_DIR_CANDIDATES = (
    Path("/var/lib/sss"),
    Path("/run/sssd"),
    Path("/var/run/sssd"),
)
NSS_SSS_LIBRARY_NAMES = (
    "libnss_sss.so.2",
    "libsss_nss_idmap.so.0",
    "libsss_idmap.so.0",
)
NSS_LIBRARY_DIR_CANDIDATES = (
    Path("/lib64"),
    Path("/usr/lib64"),
    Path("/lib/x86_64-linux-gnu"),
    Path("/usr/lib/x86_64-linux-gnu"),
    Path("/lib"),
    Path("/usr/lib"),
)
SLURM_ACCOUNT_ENV = "SBATCH_ACCOUNT"
SLURM_QOS_ENV = "SBATCH_QOS"
KUB_MPI_MODULES_ENV = "KUB_MPI_MODULES"
KUB_MPI_EXEC_MODE_ENV = "KUB_MPI_EXEC_MODE"
KUB_MPI_EXEC_MODE_PREFER_MPI = "prefer-mpi"
APPTAINER_CACHE_ENV = "APPTAINER_CACHEDIR"
APPTAINER_TMP_ENV = "APPTAINER_TMPDIR"
APPTAINER_CONFIG_ENV = "APPTAINER_CONFIGDIR"
SINGULARITY_CACHE_ENV = "SINGULARITY_CACHEDIR"
SINGULARITY_TMP_ENV = "SINGULARITY_TMPDIR"
SINGULARITY_CONFIG_ENV = "SINGULARITY_CONFIGDIR"


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
    forwardedArgsBeforePolicyRewrite = list(rewrittenArgs)
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
    ensureSlurmAccountingEnv(
        envAssignments=updatedEnvVars,
        forwardedArgs=forwardedArgsBeforePolicyRewrite,
        hostCemdbRoot=Path(hostCemdbRoot),
    )
    ensureAutoMpiExecutionEnv(
        envAssignments=updatedEnvVars,
        forwardedArgs=forwardedArgsBeforePolicyRewrite,
        hostCemdbRoot=Path(hostCemdbRoot),
        runtimeCwd=runtimeCwd,
        configHint=configHint,
    )
    if policy.shouldSyncConfigProjection():
        if policy.shouldExposeInnerRuntimeExecutable(rewrittenArgs):
            ensureApptainerRuntimeStorageEnv(
                envAssignments=updatedEnvVars,
                hostCemdbRoot=Path(hostCemdbRoot),
                configHint=configHint,
            )
        hostBridgeDir = ensureHostSlurmBridge(hostCemdbRoot=Path(hostCemdbRoot))
        if hostBridgeDir is not None:
            addBindIfMissing(
                updatedBinds,
                source=str(hostBridgeDir),
                destination=str(hostBridgeDir),
            )
            prependPathEnvAssignment(updatedEnvVars, str(hostBridgeDir))
            prependPathEnvAssignment(updatedEnvVars, SLURM_HOST_BRIDGE_CONTAINER_DIR)
            ensureEnvAssignment(updatedEnvVars, "SBATCH_EXPORT", "ALL")
            exposeHostSlurmSupportFiles(
                bindSpecs=updatedBinds,
                envAssignments=updatedEnvVars,
            )
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

    for value in candidates:
        candidatePath = Path(value).expanduser()
        hasPathSeparator = candidatePath.parent != Path(".")

        if candidatePath.is_absolute() or hasPathSeparator:
            resolved = tryResolveRunnerExecutable(value, runtimeName="apptainer")
        else:
            discovered = findExecutable(value)
            if discovered is None:
                continue
            resolved = tryResolveRunnerExecutable(
                discovered,
                runtimeName="apptainer",
            )

        if resolved is None:
            continue

        resolvedPath = Path(resolved).expanduser()
        if not resolvedPath.is_absolute():
            continue
        if resolvedPath.exists() and os.access(resolvedPath, os.X_OK):
            return resolvedPath.resolve()

    discovered = findExecutable("apptainer")
    if discovered is None:
        return None

    resolvedDiscovered = tryResolveRunnerExecutable(
        discovered,
        runtimeName="apptainer",
    )
    if resolvedDiscovered is None:
        return None

    discoveredPath = Path(resolvedDiscovered).expanduser()
    if discoveredPath.is_absolute() and discoveredPath.exists() and os.access(discoveredPath, os.X_OK):
        return discoveredPath.resolve()

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


def exposeHostSlurmSupportFiles(
    *,
    bindSpecs: list[str],
    envAssignments: list[str] | None = None,
) -> None:
    for directory in SLURM_LIBRARY_DIR_CANDIDATES:
        if directory.is_dir():
            addBindIfMissing(
                bindSpecs,
                source=str(directory),
                destination=str(directory),
            )
            if envAssignments is not None:
                prependLibraryPathEnvAssignment(envAssignments, str(directory))

    for directory in discoverHostSlurmLibraryDirectories():
        addBindIfMissing(
            bindSpecs,
            source=str(directory),
            destination=str(directory),
        )
        if envAssignments is not None:
            prependLibraryPathEnvAssignment(envAssignments, str(directory))

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

    ensureSssIdentityLookupSupport(
        bindSpecs=bindSpecs,
        envAssignments=envAssignments,
    )


def ensureSlurmAccountingEnv(
    *,
    envAssignments: list[str],
    forwardedArgs: Sequence[str],
    hostCemdbRoot: Path,
) -> None:
    if not shouldAttemptSlurmAccountingDetection(forwardedArgs):
        return

    hasCustomConfig = getForwardedOptionValue(forwardedArgs, "--config") is not None
    hasExplicitPartition = getForwardedOptionValue(forwardedArgs, "--partition") is not None
    if hasCustomConfig and not hasExplicitPartition:
        return

    if hasEnvAssignment(envAssignments, SLURM_ACCOUNT_ENV) and hasEnvAssignment(
        envAssignments,
        SLURM_QOS_ENV,
    ):
        return

    targetPartition = resolveTargetSlurmPartition(
        forwardedArgs=forwardedArgs,
        hostCemdbRoot=hostCemdbRoot,
    )
    if targetPartition is None:
        return

    association = resolveSlurmAssociationForPartition(targetPartition)
    if association is None:
        return

    account, qos = association
    if account and not hasEnvAssignment(envAssignments, SLURM_ACCOUNT_ENV):
        ensureEnvAssignment(
            envAssignments,
            SLURM_ACCOUNT_ENV,
            account,
        )
    if qos and not hasEnvAssignment(envAssignments, SLURM_QOS_ENV):
        ensureEnvAssignment(
            envAssignments,
            SLURM_QOS_ENV,
            qos,
        )


def ensureAutoMpiExecutionEnv(
    *,
    envAssignments: list[str],
    forwardedArgs: Sequence[str],
    hostCemdbRoot: Path,
    runtimeCwd: Path,
    configHint: KubConfig | None,
) -> None:
    if not shouldAttemptAutoMpiDetection(
        forwardedArgs=forwardedArgs,
        hostCemdbRoot=hostCemdbRoot,
    ):
        return

    imagePath = resolveTargetApptainerImagePath(
        forwardedArgs=forwardedArgs,
        hostCemdbRoot=hostCemdbRoot,
        runtimeCwd=runtimeCwd,
    )
    if imagePath is None:
        return

    apptainerExecutable = resolveHostApptainerExecutablePath(configHint=configHint)
    if apptainerExecutable is None:
        return

    openMpiVersion = detectImageOpenMpiVersion(
        apptainerExecutable=apptainerExecutable,
        imagePath=imagePath,
    )
    if openMpiVersion is None:
        return

    if not hasEffectiveEnvAssignment(
        assignments=envAssignments,
        key=KUB_MPI_MODULES_ENV,
        configHint=configHint,
    ):
        moduleCandidates = discoverAvailableOpenMpiModules()
        selectedModule = selectClosestOpenMpiModule(
            moduleCandidates=moduleCandidates,
            targetVersion=openMpiVersion,
        )
        if selectedModule is not None:
            ensureEnvAssignment(
                envAssignments,
                KUB_MPI_MODULES_ENV,
                selectedModule,
            )

    if not hasEffectiveEnvAssignment(
        assignments=envAssignments,
        key=KUB_MPI_EXEC_MODE_ENV,
        configHint=configHint,
    ):
        ensureEnvAssignment(
            envAssignments,
            KUB_MPI_EXEC_MODE_ENV,
            KUB_MPI_EXEC_MODE_PREFER_MPI,
        )


def ensureApptainerRuntimeStorageEnv(
    *,
    envAssignments: list[str],
    hostCemdbRoot: Path,
    configHint: KubConfig | None,
) -> None:
    stateRoot = hostCemdbRoot / ".kub-cli" / "apptainer"
    cacheDir = stateRoot / "cache"
    tmpDir = stateRoot / "tmp"
    configDir = stateRoot / "config"

    for directory in (cacheDir, tmpDir, configDir):
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ConfigError(
                f"Unable to prepare Apptainer runtime directory '{directory}': {error}"
            ) from error

    ensureApptainerEnvPair(
        envAssignments=envAssignments,
        configHint=configHint,
        primaryKey=APPTAINER_CACHE_ENV,
        legacyKey=SINGULARITY_CACHE_ENV,
        defaultValue=str(cacheDir),
    )
    ensureApptainerEnvPair(
        envAssignments=envAssignments,
        configHint=configHint,
        primaryKey=APPTAINER_TMP_ENV,
        legacyKey=SINGULARITY_TMP_ENV,
        defaultValue=str(tmpDir),
    )
    ensureApptainerEnvPair(
        envAssignments=envAssignments,
        configHint=configHint,
        primaryKey=APPTAINER_CONFIG_ENV,
        legacyKey=SINGULARITY_CONFIG_ENV,
        defaultValue=str(configDir),
    )


def ensureApptainerEnvPair(
    *,
    envAssignments: list[str],
    configHint: KubConfig | None,
    primaryKey: str,
    legacyKey: str,
    defaultValue: str,
) -> None:
    primaryValue = getEffectiveEnvValue(
        assignments=envAssignments,
        key=primaryKey,
        configHint=configHint,
    )
    normalizedPrimary = (primaryValue or "").strip()
    if not normalizedPrimary:
        normalizedPrimary = defaultValue
        ensureEnvAssignment(envAssignments, primaryKey, normalizedPrimary)

    legacyValue = getEffectiveEnvValue(
        assignments=envAssignments,
        key=legacyKey,
        configHint=configHint,
    )
    if not (legacyValue or "").strip():
        ensureEnvAssignment(envAssignments, legacyKey, normalizedPrimary)


def shouldAttemptSlurmAccountingDetection(forwardedArgs: Sequence[str]) -> bool:
    launcherValue = getForwardedOptionValue(forwardedArgs, "--launcher")
    if launcherValue is not None:
        normalizedLauncher = launcherValue.strip().lower()
        if normalizedLauncher == "slurm":
            return True
        if normalizedLauncher == "local":
            return False

    profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
    if profileValue is not None and "slurm" in profileValue.strip().lower():
        return True

    partitionValue = getForwardedOptionValue(forwardedArgs, "--partition")
    if partitionValue is not None and partitionValue.strip():
        return True

    return False


def shouldAttemptAutoMpiDetection(
    *,
    forwardedArgs: Sequence[str],
    hostCemdbRoot: Path,
) -> bool:
    if not shouldAttemptSlurmAccountingDetection(forwardedArgs):
        return False

    runtimeValue = getForwardedOptionValue(forwardedArgs, "--runtime")
    if runtimeValue is not None:
        normalized = runtimeValue.strip().lower()
        if normalized == "apptainer":
            return True
        if normalized == "native":
            return False

    if getForwardedOptionValue(forwardedArgs, "--apptainer-image") is not None:
        return True

    profileName = getForwardedOptionValue(forwardedArgs, "--profile")
    if profileName is None or not profileName.strip():
        return False

    profileRuntime = resolveProfileValueFromSimulateConfig(
        profileName=profileName.strip(),
        key="runtime",
        hostCemdbRoot=hostCemdbRoot,
    )
    if profileRuntime is None:
        return "apptainer" in profileName.strip().lower()
    return profileRuntime.strip().lower() == "apptainer"


def resolveTargetApptainerImagePath(
    *,
    forwardedArgs: Sequence[str],
    hostCemdbRoot: Path,
    runtimeCwd: Path,
) -> Path | None:
    explicitImage = getForwardedOptionValue(forwardedArgs, "--apptainer-image")
    if explicitImage is not None and explicitImage.strip():
        return resolveImagePathForHost(
            rawPath=explicitImage.strip(),
            hostCemdbRoot=hostCemdbRoot,
            runtimeCwd=runtimeCwd,
        )

    profileName = getForwardedOptionValue(forwardedArgs, "--profile")
    if profileName is None or not profileName.strip():
        return None

    profileImage = resolveProfileValueFromSimulateConfig(
        profileName=profileName.strip(),
        key="apptainer_image",
        hostCemdbRoot=hostCemdbRoot,
    )
    if profileImage is None:
        profileImage = resolveProfileValueFromSimulateConfig(
            profileName=profileName.strip(),
            key="apptainerImage",
            hostCemdbRoot=hostCemdbRoot,
        )
    if profileImage is None:
        return None

    return resolveImagePathForHost(
        rawPath=profileImage,
        hostCemdbRoot=hostCemdbRoot,
        runtimeCwd=runtimeCwd,
    )


def resolveImagePathForHost(*, rawPath: str, hostCemdbRoot: Path, runtimeCwd: Path) -> Path | None:
    if not rawPath.strip():
        return None

    raw = Path(rawPath).expanduser()
    if raw.is_absolute():
        candidate = raw
    else:
        candidate = (runtimeCwd / raw).resolve()
        if not candidate.exists():
            candidate = (hostCemdbRoot / raw).resolve()

    if not candidate.exists() or candidate.is_dir():
        return None
    return candidate


def detectImageOpenMpiVersion(*, apptainerExecutable: Path, imagePath: Path) -> str | None:
    output = runCommandCaptureStdout(
        [
            str(apptainerExecutable),
            "exec",
            str(imagePath),
            "mpirun",
            "--version",
        ]
    )
    if output is None:
        return None

    for line in output.splitlines():
        match = re.search(r"Open MPI\)\s+([0-9]+(?:\.[0-9]+){1,2})", line)
        if match is not None:
            return match.group(1)
    return None


def discoverAvailableOpenMpiModules() -> tuple[str, ...]:
    bashPath = resolveHostExecutablePath("bash")
    if bashPath is None:
        return ()

    output = runCommandCaptureStdout(
        [
            str(bashPath),
            "-lc",
            "module -t avail 2>&1 || true",
        ]
    )
    if output is None:
        return ()

    modules: list[str] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.endswith(":"):
            continue
        if stripped.lower() == "openmpi/":
            continue
        if not stripped.lower().startswith("openmpi/"):
            continue
        modules.append(stripped)

    return tuple(modules)


def selectClosestOpenMpiModule(
    *,
    moduleCandidates: Sequence[str],
    targetVersion: str,
) -> str | None:
    targetTuple = parseVersionTuple(targetVersion)
    if targetTuple is None:
        return None

    exactMatches: list[str] = []
    sameMinor: list[tuple[int, str]] = []

    for moduleName in moduleCandidates:
        moduleVersion = parseOpenMpiModuleVersion(moduleName)
        if moduleVersion is None:
            continue
        if moduleVersion == targetTuple:
            exactMatches.append(moduleName)
            continue
        if moduleVersion[:2] == targetTuple[:2]:
            patchDistance = abs(moduleVersion[2] - targetTuple[2])
            sameMinor.append((patchDistance, moduleName))

    if exactMatches:
        return exactMatches[0]

    if sameMinor:
        sameMinor.sort(key=lambda item: item[0])
        return sameMinor[0][1]

    return None


def parseOpenMpiModuleVersion(moduleName: str) -> tuple[int, int, int] | None:
    match = re.search(r"(?i)^openmpi/([0-9]+(?:\.[0-9]+){1,2})", moduleName.strip())
    if match is None:
        return None
    return parseVersionTuple(match.group(1))


def parseVersionTuple(versionText: str) -> tuple[int, int, int] | None:
    parts = versionText.strip().split(".")
    if len(parts) < 2 or len(parts) > 3:
        return None

    numericParts: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        numericParts.append(int(part))

    while len(numericParts) < 3:
        numericParts.append(0)

    return tuple(numericParts)  # type: ignore[return-value]


def hasEffectiveEnvAssignment(
    *,
    assignments: Sequence[str],
    key: str,
    configHint: KubConfig | None,
) -> bool:
    return bool(getEffectiveEnvValue(assignments=assignments, key=key, configHint=configHint))


def getEffectiveEnvValue(
    *,
    assignments: Sequence[str],
    key: str,
    configHint: KubConfig | None,
) -> str | None:
    assigned = getEnvAssignmentValue(assignments, key)
    if assigned is not None:
        return assigned
    if configHint is not None:
        configured = configHint.env.get(key)
        if configured is not None:
            return configured
    return os.environ.get(key)


def resolveTargetSlurmPartition(
    *,
    forwardedArgs: Sequence[str],
    hostCemdbRoot: Path,
) -> str | None:
    explicitPartition = getForwardedOptionValue(forwardedArgs, "--partition")
    if explicitPartition is not None and explicitPartition.strip():
        return explicitPartition.strip()

    profileName = getForwardedOptionValue(forwardedArgs, "--profile")
    if profileName is None or not profileName.strip():
        return None

    return resolvePartitionFromSimulateConfig(
        profileName=profileName.strip(),
        hostCemdbRoot=hostCemdbRoot,
    )


def resolvePartitionFromSimulateConfig(*, profileName: str, hostCemdbRoot: Path) -> str | None:
    return resolveProfileValueFromSimulateConfig(
        profileName=profileName,
        key="partition",
        hostCemdbRoot=hostCemdbRoot,
    )


def resolveProfileValueFromSimulateConfig(
    *,
    profileName: str,
    key: str,
    hostCemdbRoot: Path,
) -> str | None:
    for configPath in getSimulateConfigCandidates(hostCemdbRoot):
        configData = loadTomlMapping(configPath)
        if not configData:
            continue

        profilesRaw = configData.get("profiles")
        if isinstance(profilesRaw, dict):
            profileRaw = profilesRaw.get(profileName)
            if isinstance(profileRaw, dict):
                profileValue = profileRaw.get(key)
                if isinstance(profileValue, str) and profileValue.strip():
                    return profileValue.strip()

        defaultsRaw = configData.get("defaults")
        if isinstance(defaultsRaw, dict):
            defaultValue = defaultsRaw.get(key)
            if isinstance(defaultValue, str) and defaultValue.strip():
                return defaultValue.strip()

    return None


def getSimulateConfigCandidates(hostCemdbRoot: Path) -> tuple[Path, ...]:
    return (
        hostCemdbRoot / SIMULATE_HOST_CONFIG_FILENAME,
        hostCemdbRoot / "cemdb" / SIMULATE_HOST_CONFIG_FILENAME,
    )


def loadTomlMapping(pathValue: Path) -> dict[str, object] | None:
    if not pathValue.exists() or pathValue.is_dir():
        return None
    try:
        parsed = tomllib.loads(pathValue.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def resolveSlurmAssociationForPartition(partition: str) -> tuple[str, str] | None:
    sacctmgrPath = findExecutable("sacctmgr")
    if sacctmgrPath is None:
        return None

    userName = (
        os.environ.get("USER")
        or os.environ.get("LOGNAME")
        or ""
    ).strip()
    if not userName:
        return None

    output = runCommandCaptureStdout(
        [
            sacctmgrPath,
            "-nP",
            "show",
            "assoc",
            "where",
            f"user={userName}",
            "format=Account,Partition,QOS",
        ]
    )
    if output is None:
        return None

    return selectAssociationForPartition(output=output, partition=partition)


def runCommandCaptureStdout(command: Sequence[str]) -> str | None:
    try:
        process = subprocess.Popen(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except OSError:
        return None

    output, _ = process.communicate()
    if process.returncode != 0:
        return None
    return output


def selectAssociationForPartition(*, output: str, partition: str) -> tuple[str, str] | None:
    normalizedPartition = partition.strip()
    if not normalizedPartition:
        return None

    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        fields = stripped.split("|")
        if len(fields) < 2:
            continue

        account = fields[0].strip() if len(fields) >= 1 else ""
        assocPartition = fields[1].strip() if len(fields) >= 2 else ""
        qos = fields[2].strip() if len(fields) >= 3 else ""
        if not account:
            continue
        if assocPartition != normalizedPartition:
            continue

        return account, qos

    return None


def discoverHostSlurmLibraryDirectories() -> tuple[Path, ...]:
    discovered: list[Path] = []
    for commandName in SLURM_SHIM_COMMANDS:
        executable = findExecutable(commandName)
        if executable is None:
            continue
        discovered.extend(
            discoverLinkedLibraryDirectories(
                executablePath=Path(executable),
                includePattern="slurm",
            )
        )
    return uniquePathsInOrder(discovered)


def discoverLinkedLibraryDirectories(
    *,
    executablePath: Path,
    includePattern: str | None = None,
) -> tuple[Path, ...]:
    if not executablePath.exists():
        return ()

    try:
        process = subprocess.Popen(
            ["ldd", str(executablePath)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError:
        return ()
    output, _ = process.communicate()
    if process.returncode not in {0, 1}:
        return ()

    discovered: list[Path] = []
    for line in output.splitlines():
        match = re.search(r"^\s*(\S+)\s+=>\s+(\S+)", line)
        if match is None:
            continue

        libraryName = match.group(1)
        libraryPathRaw = match.group(2)
        if libraryPathRaw == "not":
            continue
        if not libraryPathRaw.startswith("/"):
            continue
        if includePattern is not None:
            normalizedPattern = includePattern.lower()
            if (
                normalizedPattern not in libraryName.lower()
                and normalizedPattern not in libraryPathRaw.lower()
            ):
                continue

        libraryPath = Path(libraryPathRaw)
        if not libraryPath.is_file():
            continue
        discovered.append(libraryPath.parent.resolve())

    return uniquePathsInOrder(discovered)


def uniquePathsInOrder(paths: Sequence[Path]) -> tuple[Path, ...]:
    unique: list[Path] = []
    seen: set[str] = set()
    for pathValue in paths:
        normalized = str(pathValue)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(pathValue)
    return tuple(unique)


def ensureSssIdentityLookupSupport(
    *,
    bindSpecs: list[str],
    envAssignments: list[str] | None,
) -> None:
    if not hostUsesSssIdentity():
        return

    for directory in SSSD_RUNTIME_DIR_CANDIDATES:
        if not directory.is_dir():
            continue
        addBindIfMissing(
            bindSpecs,
            source=str(directory),
            destination=str(directory),
        )

    for libraryPath in discoverSssLibraryPaths():
        addBindIfMissing(
            bindSpecs,
            source=str(libraryPath),
            destination=str(libraryPath),
        )
        if envAssignments is not None:
            prependLibraryPathEnvAssignment(
                envAssignments,
                str(libraryPath.parent),
            )


def hostUsesSssIdentity() -> bool:
    for filePath in SLURM_IDENTITY_FILE_CANDIDATES:
        if filePath.name != "nsswitch.conf" or not filePath.is_file():
            continue
        try:
            lines = filePath.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if not (stripped.startswith("passwd:") or stripped.startswith("group:")):
                continue
            _, value = stripped.split(":", maxsplit=1)
            if "sss" in value.split():
                return True
        return False
    return False


def discoverSssLibraryPaths() -> tuple[Path, ...]:
    discovered: list[Path] = []
    for libraryName in NSS_SSS_LIBRARY_NAMES:
        libraryPath = resolveLibraryPath(libraryName)
        if libraryPath is not None:
            discovered.append(libraryPath)
    return uniquePathsInOrder(discovered)


def resolveLibraryPath(libraryName: str) -> Path | None:
    for directory in NSS_LIBRARY_DIR_CANDIDATES:
        candidate = directory / libraryName
        if candidate.is_file():
            return candidate

    try:
        process = subprocess.Popen(
            ["ldconfig", "-p"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError:
        return None
    output, _ = process.communicate()
    if process.returncode != 0:
        return None

    pattern = re.compile(rf"\b{re.escape(libraryName)}\b.*=>\s+(\S+)")
    for line in output.splitlines():
        match = pattern.search(line)
        if match is None:
            continue
        candidate = Path(match.group(1))
        if candidate.is_file():
            return candidate

    return None


def ensureHostSlurmBridge(*, hostCemdbRoot: Path) -> Path | None:
    sbatchPath = resolveHostExecutablePath("sbatch")
    if sbatchPath is None:
        return None
    srunPath = resolveHostExecutablePath("srun")

    hostBinDir = hostCemdbRoot / ".kub-cli" / "host-bin"
    try:
        hostBinDir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigError(
            f"Unable to create host slurm bridge directory '{hostBinDir}': {error}"
        ) from error

    copyExecutableToBridge(
        sourcePath=sbatchPath,
        destination=hostBinDir / "sbatch",
    )

    srunRealPath = hostBinDir / "srun.real"
    if srunPath is not None:
        copyExecutableToBridge(
            sourcePath=srunPath,
            destination=srunRealPath,
        )
    elif srunRealPath.exists():
        try:
            srunRealPath.unlink()
        except OSError as error:
            raise ConfigError(
                f"Unable to remove stale slurm bridge command '{srunRealPath}': {error}"
            ) from error

    writeSrunBridgeCommandShim(
        destination=hostBinDir / "srun",
        hostBinDir=hostBinDir,
        preferredExecutable=srunPath,
        fallbackExecutables=("mpirun", "mpiexec"),
    )

    return hostBinDir


def resolveHostExecutablePath(commandName: str) -> Path | None:
    resolved = findExecutable(commandName)
    if resolved is None:
        return None

    sourcePath = Path(resolved)
    if not sourcePath.exists() or not os.access(sourcePath, os.X_OK):
        return None
    return sourcePath.resolve()


def copyExecutableToBridge(
    *,
    sourcePath: Path,
    destination: Path,
) -> None:
    if destination.exists():
        try:
            if destination.samefile(sourcePath):
                destination.chmod(0o755)
                return
        except OSError:
            pass
    try:
        shutil.copy2(sourcePath, destination)
        destination.chmod(0o755)
    except OSError as error:
        raise ConfigError(
            f"Unable to prepare host slurm bridge command '{destination}': {error}"
        ) from error


def writeSrunBridgeCommandShim(
    *,
    destination: Path,
    hostBinDir: Path,
    preferredExecutable: Path | None,
    fallbackExecutables: Sequence[str],
) -> None:
    script = buildSrunBridgeCommandShim(
        hostBinDir=hostBinDir,
        preferredExecutable=preferredExecutable,
        fallbackExecutables=fallbackExecutables,
    )
    try:
        destination.write_text(script, encoding="utf-8")
        destination.chmod(0o755)
    except OSError as error:
        raise ConfigError(
            f"Unable to prepare host slurm bridge command '{destination}': {error}"
        ) from error


def buildSrunBridgeCommandShim(
    *,
    hostBinDir: Path,
    preferredExecutable: Path | None,
    fallbackExecutables: Sequence[str],
) -> str:
    quotedLocalFallback = shlex.quote(str(hostBinDir / "srun.real"))
    lines = [
        "#!/bin/sh",
        "",
        "load_requested_modules() {",
        "  if [ -z \"${KUB_MPI_MODULES:-}\" ]; then",
        "    return 0",
        "  fi",
        "  if ! command -v module >/dev/null 2>&1; then",
        "    if [ -f /etc/profile.d/modules.sh ]; then",
        "      # shellcheck disable=SC1091",
        "      . /etc/profile.d/modules.sh",
        "    elif [ -f /usr/share/Modules/init/bash ]; then",
        "      # shellcheck disable=SC1091",
        "      . /usr/share/Modules/init/bash",
        "    fi",
        "  fi",
        "  if ! command -v module >/dev/null 2>&1; then",
        "    return 0",
        "  fi",
        "  module_list=$(printf '%s' \"${KUB_MPI_MODULES}\" | tr ',' ' ')",
        "  for module_name in ${module_list}; do",
        "    module load \"${module_name}\" >/dev/null 2>&1 || true",
        "  done",
        "}",
        "",
        "run_with_mpi_fallback() {",
        "  load_requested_modules",
    ]
    for fallback in fallbackExecutables:
        quotedFallback = shlex.quote(fallback)
        lines.extend(
            [
                f"if command -v {quotedFallback} >/dev/null 2>&1; then",
                f"  exec {quotedFallback} \"$@\"",
                "fi",
            ]
        )

    lines.extend(
        [
            "  return 127",
            "}",
            "",
            "if [ \"${KUB_MPI_EXEC_MODE:-auto}\" = \"prefer-mpi\" ]; then",
            "  run_with_mpi_fallback \"$@\"",
            "fi",
            "",
            f"if [ -x {quotedLocalFallback} ]; then",
            f"  exec {quotedLocalFallback} \"$@\"",
            "fi",
        ]
    )

    if preferredExecutable is not None:
        quotedExecutable = shlex.quote(str(preferredExecutable))
        lines.extend(
            [
                f"if [ -x {quotedExecutable} ]; then",
                f"  exec {quotedExecutable} \"$@\"",
                "fi",
            ]
        )

    lines.extend(
        [
            "",
            "run_with_mpi_fallback \"$@\"",
            "echo \"kub-cli shim: unable to resolve srun executable.\" >&2",
            "exit 127",
            "",
        ]
    )
    return "\n".join(lines)


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
        if pathContainsEntry(existingPath, prefixPath):
            return
        setEnvAssignmentValue(assignments, "PATH", f"{prefixPath}:{existingPath}")
        return

    setEnvAssignmentValue(assignments, "PATH", f"{prefixPath}:{DEFAULT_CONTAINER_PATH}")


def prependLibraryPathEnvAssignment(assignments: list[str], prefixPath: str) -> None:
    existingPath = getEnvAssignmentValue(assignments, "LD_LIBRARY_PATH")
    if existingPath is not None:
        if pathContainsEntry(existingPath, prefixPath):
            return
        setEnvAssignmentValue(assignments, "LD_LIBRARY_PATH", f"{prefixPath}:{existingPath}")
        return

    setEnvAssignmentValue(assignments, "LD_LIBRARY_PATH", prefixPath)


def pathContainsEntry(pathValue: str, entry: str) -> bool:
    return any(part == entry for part in pathValue.split(":"))


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
