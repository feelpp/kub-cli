# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from kub_cli.config import KubConfig
from kub_cli.errors import ImageNotFoundError, RunnerNotFoundError, RuntimeSelectionError
from kub_cli.runtime import (
    ApptainerCommandBuilder,
    DockerCommandBuilder,
    KubAppRunner,
    deriveApptainerOrasReference,
    resolveRuntimeForExecution,
)


def testApptainerCommandBuilderConstructsExpectedCommand(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        image=str(imagePath),
        binds=("/data:/data", "/scratch:/scratch"),
        workdir="/workspace",
        apptainerFlags=("--nv", "--writable-tmpfs"),
        env={"OMP_NUM_THREADS": "8"},
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    builder = ApptainerCommandBuilder(appName="kub-simulate", config=config)
    command = builder.build(["run", "case.yaml", "--mesh", "fine"])

    assert command == [
        "/usr/bin/apptainer",
        "run",
        "--nv",
        "--writable-tmpfs",
        "--bind",
        "/data:/data",
        "--bind",
        "/scratch:/scratch",
        "--pwd",
        "/workspace",
        "--app",
        "kub-simulate",
        str(imagePath),
        "run",
        "case.yaml",
        "--mesh",
        "fine",
    ]


def testDockerCommandBuilderConstructsExpectedCommand(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
        binds=("/data:/data", "/scratch:/scratch"),
        workdir="/workspace",
        dockerFlags=("--pull", "always"),
        env={"OMP_NUM_THREADS": "8", "KUB_ENV": "test"},
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    builder = DockerCommandBuilder(appName="kub-simulate", config=config)
    command = builder.build(["run", "case.yaml", "--mesh", "fine"])

    assert command[:4] == ["/usr/bin/docker", "run", "--rm", "--pull"]
    assert "always" in command
    assert "--volume" in command
    assert "/data:/data" in command
    assert "/scratch:/scratch" in command
    assert "--workdir" in command
    assert "/workspace" in command
    assert "--env" in command
    assert "OMP_NUM_THREADS=8" in command
    assert "KUB_ENV=test" in command
    assert command[-6:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-simulate",
        "run",
        "case.yaml",
        "--mesh",
        "fine",
    ]


def testDeriveApptainerOrasReference() -> None:
    derived = deriveApptainerOrasReference("ghcr.io/feelpp/ktirio-urban-building:master")
    assert derived == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"


@pytest.mark.parametrize(
    "docker_ref, expected",
    [
        (
            "ghcr.io/feelpp/ktirio-urban-building",
            "oras://ghcr.io/feelpp/ktirio-urban-building:latest-sif",
        ),
        (
            "registry.example.com/ns/app:v1",
            "oras://registry.example.com/ns/app:v1-sif",
        ),
    ],
)
def testDeriveApptainerOrasReferenceVariants(docker_ref: str, expected: str) -> None:
    assert deriveApptainerOrasReference(docker_ref) == expected


def testAutoRuntimePrefersApptainerWhenAvailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="auto",
        imageApptainer=str(imagePath),
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    resolution = resolveRuntimeForExecution(config)

    assert resolution.runtime == "apptainer"
    assert resolution.imageReference == str(imagePath)


def testAutoRuntimeUsesDefaultMasterImages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(runtime="auto")

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    resolution = resolveRuntimeForExecution(config)

    assert resolution.runtime == "apptainer"
    assert resolution.imageReference == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"


def testAutoRuntimeFallsBackToDocker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = KubConfig(
        runtime="auto",
        imageApptainer=str(tmp_path / "missing.sif"),
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    resolution = resolveRuntimeForExecution(config)

    assert resolution.runtime == "docker"
    assert resolution.imageReference == "ghcr.io/feelpp/ktirio-urban-building:master"


def testSelectedRuntimeRunnerNotFoundRaises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: None)

    with pytest.raises(RunnerNotFoundError):
        resolveRuntimeForExecution(config)


def testSelectedRuntimeMissingImageRaises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(tmp_path / "missing.sif"),
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    with pytest.raises(ImageNotFoundError):
        resolveRuntimeForExecution(config)


def testAutoRuntimeFailureHasHelpfulMessage(monkeypatch: pytest.MonkeyPatch) -> None:
    config = KubConfig(runtime="auto")

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: None)

    with pytest.raises(RuntimeSelectionError, match="apptainer\\.org"):
        resolveRuntimeForExecution(config)


def testDryRunSkipsSubprocessForDocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(appName="kub-dataset", forwardedArgs=["push", "./data"], dryRun=True)

    assert exitCode == 0
    assert called["value"] is False


def testSubprocessExitCodePropagationForApptainer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(imagePath),
        env={"WRAPPED_ENV": "ENABLED"},
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        captured["check"] = check
        captured["env"] = env
        return subprocess.CompletedProcess(args=command, returncode=7)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(appName="kub-simulate", forwardedArgs=["run", "case.yaml"])

    assert exitCode == 7
    assert captured["check"] is False
    assert isinstance(captured["env"], dict)
    assert captured["env"]["WRAPPED_ENV"] == "ENABLED"  # type: ignore[index]


def testKeyboardInterruptReturns130(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(imagePath),
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    def interruptingRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise KeyboardInterrupt

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", interruptingRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(appName="kub-dashboard", forwardedArgs=["serve", "./results"])

    assert exitCode == 130
