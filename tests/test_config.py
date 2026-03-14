# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest

from kub_cli.config import KubConfigOverrides, loadKubConfig
from kub_cli.errors import ConfigError


def writeText(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def testConfigPrecedenceAndRuntimeSpecificImages(tmp_path: Path) -> None:
    cwd = tmp_path / "project"
    cwd.mkdir()

    userConfig = tmp_path / "home" / ".config" / "kub-cli" / "config.toml"
    projectConfig = cwd / ".kub-cli.toml"

    writeText(
        userConfig,
        """
[kub_cli]
runtime = "auto"
bind = ["/user_a:/container_a"]
app_runner = "runner-user"
apptainer_flags = ["--nv"]
docker_flags = ["--pull=always"]

[kub_cli.image]
default = "./user-default.sif"
docker = "ghcr.io/user/image:user"
apptainer = "./user-app.sif"

[kub_cli.env]
USER_LEVEL = "1"
""".strip(),
    )

    writeText(
        projectConfig,
        """
[kub_cli]
runtime = "apptainer"
bind = "/project_a:/container_p"
workdir = "/project-work"

[kub_cli.image]
default = "./project-default.sif"
docker = "ghcr.io/project/image:project"
apptainer = "./project-app.sif"

[kub_cli.env]
PROJECT_LEVEL = "1"
""".strip(),
    )

    env = {
        "KUB_RUNTIME": "docker",
        "KUB_IMAGE": str(tmp_path / "env-default.sif"),
        "KUB_IMAGE_DOCKER": "ghcr.io/env/image:env",
        "KUB_IMAGE_APPTAINER": str(tmp_path / "env-app.sif"),
        "KUB_BIND": "/env_a:/container_e,/env_b:/container_f",
        "KUB_WORKDIR": "/env-work",
        "KUB_APP_RUNNER": "runner-env",
        "KUB_VERBOSE": "true",
        "KUB_APPTAINER_FLAGS": "--writable-tmpfs --fakeroot",
        "KUB_DOCKER_FLAGS": "--network host --pull always",
    }

    overrides = KubConfigOverrides(
        runtime="apptainer",
        image="./cli.sif",
        binds=("/cli_a:/container_c",),
        workdir="/cli-work",
        runner="runner-cli",
        verbose=False,
        apptainerFlags=("--containall",),
        dockerFlags=("--rm",),
        env={"CLI_LEVEL": "1"},
    )

    config = loadKubConfig(
        cwd=cwd,
        env=env,
        overrides=overrides,
        userConfigPath=userConfig,
    )

    assert config.runtime == "apptainer"
    assert config.imageOverride == str((cwd / "cli.sif").resolve())
    assert config.imageDocker == "ghcr.io/env/image:env"
    assert config.imageApptainer == str((tmp_path / "env-app.sif").resolve())
    assert config.image == str((tmp_path / "env-default.sif").resolve())

    assert config.runner == "runner-cli"
    assert config.workdir == "/cli-work"
    assert config.verbose is False

    assert config.binds == (
        "/user_a:/container_a",
        "/project_a:/container_p",
        "/env_a:/container_e",
        "/env_b:/container_f",
        "/cli_a:/container_c",
    )

    assert config.apptainerFlags == (
        "--nv",
        "--writable-tmpfs",
        "--fakeroot",
        "--containall",
    )
    assert config.dockerFlags == (
        "--pull=always",
        "--network",
        "host",
        "--pull",
        "always",
        "--rm",
    )

    assert dict(config.env) == {
        "USER_LEVEL": "1",
        "PROJECT_LEVEL": "1",
        "CLI_LEVEL": "1",
    }


def testImageTableParsing(tmp_path: Path) -> None:
    cwd = tmp_path / "project"
    cwd.mkdir()

    projectConfig = cwd / ".kub-cli.toml"
    writeText(
        projectConfig,
        """
[kub_cli]
runtime = "docker"

[kub_cli.image]
docker = "ghcr.io/feelpp/ktirio-urban-building:master"
apptainer = "./images/kub.sif"
""".strip(),
    )

    config = loadKubConfig(cwd=cwd, env={}, userConfigPath=tmp_path / "missing-user.toml")

    assert config.runtime == "docker"
    assert config.imageDocker == "ghcr.io/feelpp/ktirio-urban-building:master"
    assert config.imageApptainer == str((cwd / "images" / "kub.sif").resolve())


def testLegacyImageDockerReferenceNotPathResolved(tmp_path: Path) -> None:
    cwd = tmp_path / "project"
    cwd.mkdir()

    projectConfig = cwd / ".kub-cli.toml"
    writeText(projectConfig, "image = 'ghcr.io/feelpp/ktirio-urban-building:master'\n")

    config = loadKubConfig(cwd=cwd, env={}, userConfigPath=tmp_path / "missing-user.toml")

    assert config.image == "ghcr.io/feelpp/ktirio-urban-building:master"


def testInvalidBooleanRaisesError(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        loadKubConfig(
            cwd=tmp_path,
            env={"KUB_VERBOSE": "definitely"},
            userConfigPath=tmp_path / "missing-user.toml",
        )


def testInvalidRuntimeRaisesError(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        loadKubConfig(
            cwd=tmp_path,
            env={"KUB_RUNTIME": "podman"},
            userConfigPath=tmp_path / "missing-user.toml",
        )
