# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Configuration loading and precedence rules for kub-cli."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import shlex
from typing import Any, Mapping

from .errors import ConfigError

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python < 3.11
    import tomli as tomllib  # type: ignore[import-not-found]


SUPPORTED_RUNTIMES = {"auto", "apptainer", "docker"}
DEFAULT_RUNTIME = "auto"
DEFAULT_APPTAINER_RUNNER = "apptainer"
DEFAULT_DOCKER_RUNNER = "docker"
DEFAULT_DOCKER_IMAGE = "ghcr.io/feelpp/ktirio-urban-building:master"
DEFAULT_APPTAINER_IMAGE = "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"
DEFAULT_PROJECT_CONFIG_NAME = ".kub-cli.toml"
DEFAULT_USER_CONFIG_PATH = Path("~/.config/kub-cli/config.toml")


@dataclass(frozen=True)
class KubConfig:
    """Effective kub-cli configuration used for command execution."""

    runtime: str = DEFAULT_RUNTIME
    imageOverride: str | None = None
    image: str | None = None
    imageDocker: str | None = None
    imageApptainer: str | None = None
    binds: tuple[str, ...] = ()
    workdir: str | None = None
    runner: str | None = None
    apptainerRunner: str = DEFAULT_APPTAINER_RUNNER
    dockerRunner: str = DEFAULT_DOCKER_RUNNER
    verbose: bool = False
    apptainerFlags: tuple[str, ...] = ()
    dockerFlags: tuple[str, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)

    def toDict(self) -> dict[str, Any]:
        return {
            "runtime": self.runtime,
            "imageOverride": self.imageOverride,
            "image": self.image,
            "imageDocker": self.imageDocker,
            "imageApptainer": self.imageApptainer,
            "binds": list(self.binds),
            "workdir": self.workdir,
            "runner": self.runner,
            "apptainerRunner": self.apptainerRunner,
            "dockerRunner": self.dockerRunner,
            "verbose": self.verbose,
            "apptainerFlags": list(self.apptainerFlags),
            "dockerFlags": list(self.dockerFlags),
            "env": dict(self.env),
        }


@dataclass(frozen=True)
class KubConfigOverrides:
    """CLI-provided configuration override values."""

    runtime: str | None = None
    image: str | None = None
    imageDocker: str | None = None
    imageApptainer: str | None = None
    binds: tuple[str, ...] = ()
    workdir: str | None = None
    runner: str | None = None
    apptainerRunner: str | None = None
    dockerRunner: str | None = None
    verbose: bool | None = None
    apptainerFlags: tuple[str, ...] = ()
    dockerFlags: tuple[str, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class _PartialConfig:
    runtime: str | None = None
    imageOverride: str | None = None
    image: str | None = None
    imageDocker: str | None = None
    imageApptainer: str | None = None
    binds: list[str] = field(default_factory=list)
    workdir: str | None = None
    runner: str | None = None
    apptainerRunner: str | None = None
    dockerRunner: str | None = None
    verbose: bool | None = None
    apptainerFlags: list[str] = field(default_factory=list)
    dockerFlags: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


def loadKubConfig(
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    overrides: KubConfigOverrides | None = None,
    userConfigPath: Path | None = None,
    projectConfigName: str = DEFAULT_PROJECT_CONFIG_NAME,
) -> KubConfig:
    """Load effective configuration using precedence:

    CLI options > environment variables > project config > user config > defaults.
    """

    runtimeCwd = (cwd or Path.cwd()).resolve()
    runtimeEnv = dict(os.environ if env is None else env)
    userPath = (userConfigPath or DEFAULT_USER_CONFIG_PATH).expanduser()
    projectPath = runtimeCwd / projectConfigName

    config = KubConfig()
    config = mergeConfig(config, loadFilePartial(userPath))
    config = mergeConfig(config, loadFilePartial(projectPath))
    config = mergeConfig(config, loadEnvPartial(runtimeEnv, runtimeCwd))

    if overrides is not None:
        config = mergeConfig(config, loadOverridePartial(overrides, runtimeCwd))

    return config


def loadFilePartial(path: Path) -> _PartialConfig:
    if not path.exists():
        return _PartialConfig()

    try:
        with path.open("rb") as stream:
            parsed = tomllib.load(stream)
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"Invalid TOML in config file '{path}': {error}") from error
    except OSError as error:
        raise ConfigError(f"Unable to read config file '{path}': {error}") from error

    table = extractConfigTable(parsed)
    return parseMappingAsPartial(table, baseDir=path.parent)


def loadEnvPartial(env: Mapping[str, str], cwd: Path) -> _PartialConfig:
    partial = _PartialConfig()

    runtimeRaw = env.get("KUB_RUNTIME")
    if runtimeRaw:
        partial.runtime = parseRuntime(runtimeRaw, variableName="KUB_RUNTIME")

    imageRaw = env.get("KUB_IMAGE")
    if imageRaw:
        partial.image = parseLegacyImageValue(imageRaw, baseDir=cwd)

    imageDockerRaw = env.get("KUB_IMAGE_DOCKER")
    if imageDockerRaw:
        partial.imageDocker = parseDockerImageValue(imageDockerRaw)

    imageApptainerRaw = env.get("KUB_IMAGE_APPTAINER")
    if imageApptainerRaw:
        partial.imageApptainer = parseApptainerImageValue(imageApptainerRaw, baseDir=cwd)

    bindsRaw = env.get("KUB_BIND")
    if bindsRaw:
        partial.binds = parseDelimitedList(bindsRaw)

    workdirRaw = env.get("KUB_WORKDIR")
    if workdirRaw:
        partial.workdir = workdirRaw

    runnerRaw = env.get("KUB_APP_RUNNER")
    if runnerRaw:
        partial.runner = runnerRaw

    apptainerRunnerRaw = env.get("KUB_APPTAINER_RUNNER")
    if apptainerRunnerRaw:
        partial.apptainerRunner = apptainerRunnerRaw

    dockerRunnerRaw = env.get("KUB_DOCKER_RUNNER")
    if dockerRunnerRaw:
        partial.dockerRunner = dockerRunnerRaw

    verboseRaw = env.get("KUB_VERBOSE")
    if verboseRaw is not None:
        partial.verbose = parseBool(verboseRaw, variableName="KUB_VERBOSE")

    apptainerFlagsRaw = env.get("KUB_APPTAINER_FLAGS")
    if apptainerFlagsRaw:
        partial.apptainerFlags = shlex.split(apptainerFlagsRaw)

    dockerFlagsRaw = env.get("KUB_DOCKER_FLAGS")
    if dockerFlagsRaw:
        partial.dockerFlags = shlex.split(dockerFlagsRaw)

    return partial


def loadOverridePartial(overrides: KubConfigOverrides, cwd: Path) -> _PartialConfig:
    partial = _PartialConfig()

    if overrides.runtime is not None:
        partial.runtime = parseRuntime(overrides.runtime, variableName="--runtime")

    if overrides.image is not None:
        partial.imageOverride = parseLegacyImageValue(overrides.image, baseDir=cwd)

    if overrides.imageDocker is not None:
        partial.imageDocker = parseDockerImageValue(overrides.imageDocker)

    if overrides.imageApptainer is not None:
        partial.imageApptainer = parseApptainerImageValue(
            overrides.imageApptainer,
            baseDir=cwd,
        )

    if overrides.binds:
        partial.binds = list(overrides.binds)

    if overrides.workdir is not None:
        partial.workdir = overrides.workdir

    if overrides.runner is not None:
        partial.runner = overrides.runner

    if overrides.apptainerRunner is not None:
        partial.apptainerRunner = overrides.apptainerRunner

    if overrides.dockerRunner is not None:
        partial.dockerRunner = overrides.dockerRunner

    if overrides.verbose is not None:
        partial.verbose = overrides.verbose

    if overrides.apptainerFlags:
        partial.apptainerFlags = list(overrides.apptainerFlags)

    if overrides.dockerFlags:
        partial.dockerFlags = list(overrides.dockerFlags)

    if overrides.env:
        partial.env = dict(overrides.env)

    return partial


def mergeConfig(base: KubConfig, partial: _PartialConfig) -> KubConfig:
    runtime = partial.runtime if partial.runtime is not None else base.runtime
    imageOverride = (
        partial.imageOverride if partial.imageOverride is not None else base.imageOverride
    )
    image = partial.image if partial.image is not None else base.image
    imageDocker = partial.imageDocker if partial.imageDocker is not None else base.imageDocker
    imageApptainer = (
        partial.imageApptainer
        if partial.imageApptainer is not None
        else base.imageApptainer
    )
    workdir = partial.workdir if partial.workdir is not None else base.workdir
    runner = partial.runner if partial.runner is not None else base.runner
    apptainerRunner = (
        partial.apptainerRunner
        if partial.apptainerRunner is not None
        else base.apptainerRunner
    )
    dockerRunner = (
        partial.dockerRunner if partial.dockerRunner is not None else base.dockerRunner
    )
    verbose = partial.verbose if partial.verbose is not None else base.verbose

    binds = uniqueInOrder([*base.binds, *partial.binds])
    apptainerFlags = uniqueInOrder([*base.apptainerFlags, *partial.apptainerFlags])
    dockerFlags = uniqueInOrder([*base.dockerFlags, *partial.dockerFlags])

    mergedEnv = dict(base.env)
    mergedEnv.update(partial.env)

    return KubConfig(
        runtime=runtime,
        imageOverride=imageOverride,
        image=image,
        imageDocker=imageDocker,
        imageApptainer=imageApptainer,
        binds=tuple(binds),
        workdir=workdir,
        runner=runner,
        apptainerRunner=apptainerRunner,
        dockerRunner=dockerRunner,
        verbose=verbose,
        apptainerFlags=tuple(apptainerFlags),
        dockerFlags=tuple(dockerFlags),
        env=mergedEnv,
    )


def parseMappingAsPartial(mapping: Mapping[str, Any], *, baseDir: Path) -> _PartialConfig:
    partial = _PartialConfig()

    runtimeRaw = mapping.get("runtime")
    if runtimeRaw is not None:
        partial.runtime = parseRuntime(runtimeRaw, variableName="runtime")

    imageRaw = mapping.get("image")
    if imageRaw is not None:
        if isinstance(imageRaw, Mapping):
            defaultImageRaw = imageRaw.get("default")
            if defaultImageRaw is not None:
                partial.image = parseLegacyImageValue(defaultImageRaw, baseDir=baseDir)

            dockerImageRaw = imageRaw.get("docker")
            if dockerImageRaw is not None:
                partial.imageDocker = parseDockerImageValue(dockerImageRaw)

            apptainerImageRaw = imageRaw.get("apptainer")
            if apptainerImageRaw is not None:
                partial.imageApptainer = parseApptainerImageValue(
                    apptainerImageRaw,
                    baseDir=baseDir,
                )
        else:
            partial.image = parseLegacyImageValue(imageRaw, baseDir=baseDir)

    imageDockerRaw = mapping.get("image_docker", mapping.get("imageDocker"))
    if imageDockerRaw is not None:
        partial.imageDocker = parseDockerImageValue(imageDockerRaw)

    imageApptainerRaw = mapping.get(
        "image_apptainer",
        mapping.get("imageApptainer"),
    )
    if imageApptainerRaw is not None:
        partial.imageApptainer = parseApptainerImageValue(imageApptainerRaw, baseDir=baseDir)

    bindRaw = mapping.get("bind")
    bindsRaw = mapping.get("binds")
    if bindRaw is not None:
        partial.binds.extend(parseBindValue(bindRaw))
    if bindsRaw is not None:
        partial.binds.extend(parseBindValue(bindsRaw))

    workdirRaw = mapping.get("workdir", mapping.get("pwd"))
    if workdirRaw is not None:
        partial.workdir = str(workdirRaw)

    runnerRaw = mapping.get("app_runner", mapping.get("runner"))
    if runnerRaw is not None:
        partial.runner = str(runnerRaw)

    apptainerRunnerRaw = mapping.get("apptainer_runner", mapping.get("apptainerRunner"))
    if apptainerRunnerRaw is not None:
        partial.apptainerRunner = str(apptainerRunnerRaw)

    dockerRunnerRaw = mapping.get("docker_runner", mapping.get("dockerRunner"))
    if dockerRunnerRaw is not None:
        partial.dockerRunner = str(dockerRunnerRaw)

    verboseRaw = mapping.get("verbose")
    if verboseRaw is not None:
        if isinstance(verboseRaw, bool):
            partial.verbose = verboseRaw
        elif isinstance(verboseRaw, str):
            partial.verbose = parseBool(verboseRaw, variableName="verbose")
        else:
            raise ConfigError("Config value 'verbose' must be a boolean or string")

    apptainerFlagsRaw = mapping.get("apptainer_flags", mapping.get("apptainerFlags"))
    if apptainerFlagsRaw is not None:
        partial.apptainerFlags = parseFlagValue(
            apptainerFlagsRaw,
            variableName="apptainer_flags",
        )

    dockerFlagsRaw = mapping.get("docker_flags", mapping.get("dockerFlags"))
    if dockerFlagsRaw is not None:
        partial.dockerFlags = parseFlagValue(
            dockerFlagsRaw,
            variableName="docker_flags",
        )

    envRaw = mapping.get("env")
    if envRaw is not None:
        if not isinstance(envRaw, Mapping):
            raise ConfigError("Config value 'env' must be a TOML table")
        partial.env = {str(key): str(value) for key, value in envRaw.items()}

    return partial


def extractConfigTable(parsed: Any) -> Mapping[str, Any]:
    if not isinstance(parsed, Mapping):
        raise ConfigError("Top-level TOML data must be a table")

    if "kub_cli" in parsed and isinstance(parsed["kub_cli"], Mapping):
        return parsed["kub_cli"]

    if "kub-cli" in parsed and isinstance(parsed["kub-cli"], Mapping):
        return parsed["kub-cli"]

    return parsed


def parseRuntime(rawValue: Any, *, variableName: str) -> str:
    if not isinstance(rawValue, str):
        raise ConfigError(
            f"Invalid runtime value for '{variableName}': expected string, "
            f"received {type(rawValue)!r}"
        )

    normalized = rawValue.strip().lower()
    if normalized in SUPPORTED_RUNTIMES:
        return normalized

    supported = ", ".join(sorted(SUPPORTED_RUNTIMES))
    raise ConfigError(
        f"Invalid runtime value for '{variableName}': '{rawValue}'. "
        f"Use one of: {supported}"
    )


def parseLegacyImageValue(value: Any, *, baseDir: Path) -> str:
    text = parseStringValue(value, variableName="image")
    if looksLikeContainerReference(text):
        return text
    return normalizePathString(text, baseDir=baseDir)


def parseDockerImageValue(value: Any) -> str:
    return parseStringValue(value, variableName="image.docker")


def parseApptainerImageValue(value: Any, *, baseDir: Path) -> str:
    text = parseStringValue(value, variableName="image.apptainer")
    if hasUriScheme(text):
        return text
    if looksLikeContainerReference(text):
        return f"oras://{text}"
    return normalizePathString(text, baseDir=baseDir)


def parseStringValue(value: Any, *, variableName: str) -> str:
    if not isinstance(value, (str, Path)):
        raise ConfigError(
            f"Config value '{variableName}' must be a string, "
            f"received {type(value)!r}"
        )

    normalized = str(value).strip()
    if not normalized:
        raise ConfigError(f"Config value '{variableName}' cannot be empty")

    return normalized


def normalizePathString(rawValue: str, *, baseDir: Path) -> str:
    pathValue = Path(rawValue).expanduser()
    if pathValue.is_absolute():
        return str(pathValue)
    return str((baseDir / pathValue).resolve())


def looksLikeContainerReference(value: str) -> bool:
    if hasUriScheme(value):
        return True

    if "@" in value:
        return True

    if value.startswith(("./", "../", "/", "~")):
        return False

    firstSegment = value.split("/", maxsplit=1)[0]
    if "." in firstSegment or ":" in firstSegment:
        return True

    tagPattern = re.compile(r"^[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9._-]+$")
    return bool(tagPattern.match(value))


def hasUriScheme(value: str) -> bool:
    return "://" in value


def parseBindValue(value: Any) -> list[str]:
    if isinstance(value, str):
        return parseDelimitedList(value)

    if isinstance(value, list):
        binds: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ConfigError("Bind entries must be strings")
            binds.extend(parseDelimitedList(item))
        return binds

    raise ConfigError("Bind value must be a string or list of strings")


def parseFlagValue(value: Any, *, variableName: str) -> list[str]:
    if isinstance(value, str):
        return shlex.split(value)

    if isinstance(value, list):
        flags: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ConfigError(f"{variableName} entries must be strings")
            flags.append(item)
        return flags

    raise ConfigError(f"{variableName} must be a string or list of strings")


def parseDelimitedList(rawValue: str) -> list[str]:
    normalized = rawValue.replace(";", "\n").replace(",", "\n")
    return [token.strip() for token in normalized.splitlines() if token.strip()]


def parseBool(rawValue: str, *, variableName: str) -> bool:
    lowered = rawValue.strip().lower()
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}

    if lowered in truthy:
        return True

    if lowered in falsy:
        return False

    raise ConfigError(
        f"Invalid boolean value for '{variableName}': '{rawValue}'. "
        "Use one of: true/false, 1/0, yes/no, on/off"
    )


def uniqueInOrder(values: list[str]) -> list[str]:
    seen: set[str] = set()
    uniqueValues: list[str] = []

    for value in values:
        if value in seen:
            continue
        seen.add(value)
        uniqueValues.append(value)

    return uniqueValues
