# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""System capability inspection and startup preflight checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import time
from typing import Any, Mapping, Sequence

from .app_policy import getForwardedOptionValue
from .config import KubConfig
from .errors import KubCliError
from .runtime import resolveRuntimeForExecution


DEFAULT_DOCTOR_CACHE_TTL_SECONDS = 300
DEFAULT_DOCTOR_CACHE_FILE = "system-capabilities.json"
SIMULATE_APP_NAME = "kub-simulate"


@dataclass(frozen=True)
class CapabilityCheck:
    """One capability check row in doctor/preflight output."""

    name: str
    required: bool
    status: str
    detail: str
    value: str | None = None

    def toDict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "required": self.required,
            "status": self.status,
            "detail": self.detail,
            "value": self.value,
        }

    @staticmethod
    def fromDict(payload: Mapping[str, Any]) -> CapabilityCheck | None:
        name = payload.get("name")
        required = payload.get("required")
        status = payload.get("status")
        detail = payload.get("detail")
        value = payload.get("value")
        if not isinstance(name, str):
            return None
        if not isinstance(required, bool):
            return None
        if not isinstance(status, str):
            return None
        if not isinstance(detail, str):
            return None
        if value is not None and not isinstance(value, str):
            return None
        return CapabilityCheck(
            name=name,
            required=required,
            status=status,
            detail=detail,
            value=value,
        )


@dataclass(frozen=True)
class DoctorReport:
    """Serialized report returned by `kub-cli doctor`."""

    generatedUtc: str
    fingerprint: str
    cacheHit: bool
    checks: tuple[CapabilityCheck, ...]

    def toDict(self) -> dict[str, Any]:
        requiredFailures = [
            check.toDict()
            for check in self.checks
            if check.required and check.status == "fail"
        ]
        return {
            "generatedUtc": self.generatedUtc,
            "host": socket.gethostname(),
            "fingerprint": self.fingerprint,
            "cacheHit": self.cacheHit,
            "checks": [check.toDict() for check in self.checks],
            "requiredFailureCount": len(requiredFailures),
        }

    @staticmethod
    def fromDict(payload: Mapping[str, Any]) -> DoctorReport | None:
        generatedUtc = payload.get("generatedUtc")
        fingerprint = payload.get("fingerprint")
        checksRaw = payload.get("checks")
        if not isinstance(generatedUtc, str):
            return None
        if not isinstance(fingerprint, str):
            return None
        if not isinstance(checksRaw, list):
            return None

        checks: list[CapabilityCheck] = []
        for item in checksRaw:
            if not isinstance(item, Mapping):
                return None
            parsed = CapabilityCheck.fromDict(item)
            if parsed is None:
                return None
            checks.append(parsed)

        return DoctorReport(
            generatedUtc=generatedUtc,
            fingerprint=fingerprint,
            cacheHit=False,
            checks=tuple(checks),
        )


def buildDoctorFingerprint(*, env: Mapping[str, str]) -> str:
    fingerprintPayload = {
        "PATH": env.get("PATH", ""),
        "KUB_RUNTIME": env.get("KUB_RUNTIME", ""),
        "KUB_APP_RUNNER": env.get("KUB_APP_RUNNER", ""),
        "KUB_APPTAINER_RUNNER": env.get("KUB_APPTAINER_RUNNER", ""),
        "KUB_DOCKER_RUNNER": env.get("KUB_DOCKER_RUNNER", ""),
        "USER": env.get("USER", ""),
        "LOGNAME": env.get("LOGNAME", ""),
        "HOSTNAME": socket.gethostname(),
        "PYTHON_VERSION": os.sys.version,
    }
    serialized = json.dumps(
        fingerprintPayload,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def defaultDoctorCachePath(*, env: Mapping[str, str]) -> Path:
    xdgCache = env.get("XDG_CACHE_HOME")
    if xdgCache is not None and xdgCache.strip():
        cacheRoot = Path(xdgCache).expanduser()
    else:
        cacheRoot = Path("~/.cache").expanduser()
    return cacheRoot / "kub-cli" / DEFAULT_DOCTOR_CACHE_FILE


def loadDoctorReportFromCache(cachePath: Path) -> DoctorReport | None:
    if not cachePath.exists() or cachePath.is_dir():
        return None
    try:
        payload = json.loads(cachePath.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    return DoctorReport.fromDict(payload)


def saveDoctorReportToCache(*, cachePath: Path, report: DoctorReport) -> None:
    try:
        cachePath.parent.mkdir(parents=True, exist_ok=True)
        cachePath.write_text(
            json.dumps(report.toDict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except OSError:
        return


def parseIsoUtcTimestamp(rawValue: str) -> float | None:
    try:
        parsed = datetime.fromisoformat(rawValue)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def runDoctorChecks(
    *,
    config: KubConfig,
    env: Mapping[str, str],
) -> tuple[CapabilityCheck, ...]:
    checks: list[CapabilityCheck] = []

    checks.append(
        CapabilityCheck(
            name="configured-runtime",
            required=True,
            status="ok",
            detail=f"Configured runtime is '{config.runtime}'.",
            value=config.runtime,
        )
    )

    runtimeResolution = None
    try:
        runtimeResolution = resolveRuntimeForExecution(config)
    except KubCliError as error:
        checks.append(
            CapabilityCheck(
                name="runtime-resolution",
                required=True,
                status="fail",
                detail=str(error),
            )
        )
    else:
        checks.append(
            CapabilityCheck(
                name="runtime-resolution",
                required=True,
                status="ok",
                detail=(
                    f"Resolved runtime '{runtimeResolution.runtime}' "
                    f"with runner '{runtimeResolution.runnerPath}'."
                ),
                value=runtimeResolution.runtime,
            )
        )

    for executableName in ("apptainer", "docker", "sbatch", "srun", "sacctmgr"):
        resolved = shutil.which(executableName, path=env.get("PATH"))
        checks.append(
            CapabilityCheck(
                name=f"executable:{executableName}",
                required=executableName in {"apptainer", "docker"},
                status="ok" if resolved is not None else "warn",
                detail=(
                    f"Found executable '{executableName}' at '{resolved}'."
                    if resolved is not None
                    else f"Executable '{executableName}' not found in PATH."
                ),
                value=resolved,
            )
        )

    moduleInitAvailable = (
        Path("/etc/profile.d/modules.sh").is_file()
        or Path("/usr/share/Modules/init/bash").is_file()
    )
    modulecmdPath = shutil.which("modulecmd", path=env.get("PATH"))
    if modulecmdPath is not None:
        checks.append(
            CapabilityCheck(
                name="modules-runtime",
                required=False,
                status="ok",
                detail=f"Found modulecmd at '{modulecmdPath}'.",
                value=modulecmdPath,
            )
        )
    elif moduleInitAvailable:
        checks.append(
            CapabilityCheck(
                name="modules-runtime",
                required=False,
                status="warn",
                detail=(
                    "No modulecmd executable in PATH, but shell module init scripts "
                    "are available."
                ),
            )
        )
    else:
        checks.append(
            CapabilityCheck(
                name="modules-runtime",
                required=False,
                status="warn",
                detail=(
                    "No modulecmd executable or module init scripts detected. "
                    "Auto MPI module loading may be limited."
                ),
            )
        )

    if runtimeResolution is not None and runtimeResolution.runtime == "apptainer":
        imageReference = runtimeResolution.imageReference
        if "://" not in imageReference:
            imagePath = Path(imageReference).expanduser()
            if imagePath.is_file():
                checks.append(
                    CapabilityCheck(
                        name="apptainer-image",
                        required=True,
                        status="ok",
                        detail=f"Local Apptainer image exists: '{imagePath}'.",
                        value=str(imagePath),
                    )
                )
            else:
                checks.append(
                    CapabilityCheck(
                        name="apptainer-image",
                        required=True,
                        status="fail",
                        detail=(
                            f"Local Apptainer image not found or invalid: '{imagePath}'. "
                            "Run `kub-img pull --runtime apptainer`."
                        ),
                        value=str(imagePath),
                    )
                )

    return tuple(checks)


def runSystemDoctor(
    *,
    config: KubConfig,
    env: Mapping[str, str] | None = None,
    cachePath: Path | None = None,
    cacheTtlSeconds: int = DEFAULT_DOCTOR_CACHE_TTL_SECONDS,
    refresh: bool = False,
    useCache: bool = True,
    nowEpoch: float | None = None,
) -> DoctorReport:
    runtimeEnv = dict(os.environ if env is None else env)
    fingerprint = buildDoctorFingerprint(env=runtimeEnv)
    resolvedCachePath = cachePath or defaultDoctorCachePath(env=runtimeEnv)
    nowValue = time.time() if nowEpoch is None else nowEpoch

    if useCache and not refresh:
        cached = loadDoctorReportFromCache(resolvedCachePath)
        if cached is not None and cached.fingerprint == fingerprint:
            generatedEpoch = parseIsoUtcTimestamp(cached.generatedUtc)
            if generatedEpoch is not None and nowValue - generatedEpoch <= cacheTtlSeconds:
                return DoctorReport(
                    generatedUtc=cached.generatedUtc,
                    fingerprint=cached.fingerprint,
                    cacheHit=True,
                    checks=cached.checks,
                )

    generatedUtc = datetime.fromtimestamp(nowValue, tz=timezone.utc).isoformat()
    report = DoctorReport(
        generatedUtc=generatedUtc,
        fingerprint=fingerprint,
        cacheHit=False,
        checks=runDoctorChecks(config=config, env=runtimeEnv),
    )

    if useCache:
        saveDoctorReportToCache(cachePath=resolvedCachePath, report=report)

    return report


def reportHasRequiredFailures(report: DoctorReport) -> bool:
    return any(check.required and check.status == "fail" for check in report.checks)


def formatDoctorReport(report: DoctorReport) -> str:
    lines = [
        f"Generated (UTC): {report.generatedUtc}",
        f"Host: {socket.gethostname()}",
        f"Cache: {'hit' if report.cacheHit else 'miss'}",
        "",
        "Checks:",
    ]
    for check in report.checks:
        status = check.status.upper()
        requirement = "required" if check.required else "optional"
        lines.append(f"- [{status}] {check.name} ({requirement}): {check.detail}")
    return "\n".join(lines)


def isLikelySlurmInvocation(forwardedArgs: Sequence[str]) -> bool:
    launcherValue = getForwardedOptionValue(forwardedArgs, "--launcher")
    if launcherValue is not None:
        normalized = launcherValue.strip().lower()
        if normalized == "slurm":
            return True
        if normalized == "local":
            return False

    profileValue = getForwardedOptionValue(forwardedArgs, "--profile")
    if profileValue is not None and "slurm" in profileValue.strip().lower():
        return True

    partitionValue = getForwardedOptionValue(forwardedArgs, "--partition")
    if partitionValue is not None and partitionValue.strip():
        return True

    return False


def runWrapperPreflight(
    *,
    appName: str,
    forwardedArgs: Sequence[str],
    config: KubConfig,
) -> None:
    try:
        runtimeResolution = resolveRuntimeForExecution(config)
    except KubCliError as error:
        raise KubCliError(f"Preflight failed: {error}", exit_code=error.exit_code) from error

    if runtimeResolution.runtime == "apptainer":
        imageReference = runtimeResolution.imageReference
        if "://" not in imageReference:
            imagePath = Path(imageReference).expanduser()
            if not imagePath.is_file():
                raise KubCliError(
                    "Preflight failed: local Apptainer image does not exist or is not a file. "
                    f"Path: '{imagePath}'. Run `kub-img pull --runtime apptainer`."
                )

    if appName != SIMULATE_APP_NAME:
        return

    if not isLikelySlurmInvocation(forwardedArgs):
        return

    # For simulate+slurm we only validate critical requirements here.
    # sbatch/srun fallbacks are handled later in wrapper context.
