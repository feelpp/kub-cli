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
    assert "--user" in command
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


def testDockerUserFlagIsNotInjectedWhenExplicitlyProvided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
        dockerFlags=("--user", "1234:1234"),
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    builder = DockerCommandBuilder(appName="kub-dataset", config=config)
    command = builder.build(["push", "./data"])

    assert command.count("--user") == 1
    userIndex = command.index("--user")
    assert command[userIndex + 1] == "1234:1234"


def testDockerDashboardDefaultsToHostNetwork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    builder = DockerCommandBuilder(appName="kub-dashboard", config=config)
    command = builder.build(["serve"])

    assert "--network" in command
    networkIndex = command.index("--network")
    assert command[networkIndex + 1] == "host"


def testDockerDashboardHonorsExplicitNetworkFlag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
        dockerFlags=("--network", "bridge"),
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    builder = DockerCommandBuilder(appName="kub-dashboard", config=config)
    command = builder.build(["serve"])

    assert command.count("--network") == 1
    networkIndex = command.index("--network")
    assert command[networkIndex + 1] == "bridge"


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
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
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


def testAutoRuntimeUsesLocalMasterSifWhenPresent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    localImage = tmp_path / "kub-master.sif"
    localImage.write_text("dummy", encoding="utf-8")

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
    assert resolution.imageReference == str(localImage.resolve())


def testAutoRuntimeSupportsLegacyLocalMasterSifName(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    legacyImage = tmp_path / "ktirio-urban-building-master.sif"
    legacyImage.write_text("dummy", encoding="utf-8")

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
    assert resolution.imageReference == str(legacyImage.resolve())


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


def testSelectedRuntimeRejectsBrokenApptainerRunner(
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
    monkeypatch.setattr(
        "kub_cli.runtime.probeRunnerExecutable",
        lambda runnerPath, *, runtimeName: (
            "startup probe exited with code 255: "
            "FATAL: While initializing: couldn't parse configuration file "
            "/etc/apptainer/apptainer.conf"
        ),
    )

    with pytest.raises(RunnerNotFoundError, match="not usable"):
        resolveRuntimeForExecution(config)


def testAutoRuntimeFallsBackToDockerWhenApptainerRunnerProbeFails(
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

    def fakeProbe(runnerPath: str, *, runtimeName: str) -> str | None:
        if runtimeName == "apptainer":
            return (
                "startup probe exited with code 255: "
                "FATAL: While initializing: couldn't parse configuration file "
                "/etc/apptainer/apptainer.conf"
            )
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)
    monkeypatch.setattr("kub_cli.runtime.probeRunnerExecutable", fakeProbe)

    resolution = resolveRuntimeForExecution(config)

    assert resolution.runtime == "docker"
    assert resolution.runnerPath == "/usr/bin/docker"


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


def testSelectedDockerRuntimeRejectsInvalidExplicitImageOverride(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="docker",
        imageOverride="/tmp/custom.sif",
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    with pytest.raises(ImageNotFoundError, match="Invalid Docker image reference"):
        resolveRuntimeForExecution(config)


def testAutoRuntimeIgnoresLocalImageOverrideWhenFallingBackToDocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubConfig(
        runtime="auto",
        imageOverride="/tmp/custom.sif",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    def fakeWhich(name: str) -> str | None:
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    resolution = resolveRuntimeForExecution(config)

    assert resolution.runtime == "docker"
    assert resolution.imageReference == "ghcr.io/feelpp/ktirio-urban-building:master"


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


def testDryRunSkipsApptainerInspectProbe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(imagePath),
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(
        appName="kub-simulate",
        forwardedArgs=["status", "arz"],
        dryRun=True,
    )

    assert exitCode == 0
    assert called["value"] is False
    output = capsys.readouterr().out
    assert "apptainer run" in output
    assert "--app kub-simulate" in output


def testSubprocessExitCodePropagationForApptainer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(imagePath),
        env={"WRAPPED_ENV": "ENABLED", "HOME": "/cemdb"},
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRun(command, check=False, env=None, capture_output=False, text=False):  # type: ignore[no-untyped-def]
        if len(command) >= 3 and command[1] == "inspect" and command[2] == "--list-apps":
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="kub-simulate\nkub-dataset\nkub-dashboard\n",
                stderr="",
            )

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
    assert captured["env"]["APPTAINERENV_WRAPPED_ENV"] == "ENABLED"  # type: ignore[index]
    assert captured["env"]["SINGULARITYENV_WRAPPED_ENV"] == "ENABLED"  # type: ignore[index]
    assert captured["env"]["HOME"] == "/cemdb"  # type: ignore[index]
    assert "APPTAINERENV_HOME" not in captured["env"]  # type: ignore[operator]
    assert "SINGULARITYENV_HOME" not in captured["env"]  # type: ignore[operator]


def testApptainerPathEnvIsForwardedWithRuntimePrefixes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    config = KubConfig(
        runtime="apptainer",
        imageApptainer=str(imagePath),
        env={"PATH": "/cemdb/.kub-cli/shims:/usr/bin:/bin"},
    )

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRun(command, check=False, env=None, capture_output=False, text=False):  # type: ignore[no-untyped-def]
        if len(command) >= 3 and command[1] == "inspect" and command[2] == "--list-apps":
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="kub-simulate\nkub-dataset\nkub-dashboard\n",
                stderr="",
            )

        captured["env"] = env
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(appName="kub-simulate", forwardedArgs=["status", "arz"])

    assert exitCode == 0
    assert isinstance(captured["env"], dict)
    assert captured["env"]["PATH"] == "/cemdb/.kub-cli/shims:/usr/bin:/bin"  # type: ignore[index]
    assert (  # type: ignore[index]
        captured["env"]["APPTAINERENV_PATH"] == "/cemdb/.kub-cli/shims:/usr/bin:/bin"
    )
    assert (  # type: ignore[index]
        captured["env"]["SINGULARITYENV_PATH"] == "/cemdb/.kub-cli/shims:/usr/bin:/bin"
    )


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


def testApptainerExecFallbackIsUsedWhenAppIsMissing(
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

    calls: list[list[str]] = []

    def fakeRun(command, check=False, env=None, capture_output=False, text=False):  # type: ignore[no-untyped-def]
        calls.append(list(command))
        if len(command) >= 3 and command[1] == "inspect" and command[2] == "--list-apps":
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="other-app\n",
                stderr="",
            )
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    runner = KubAppRunner(config=config)
    exitCode = runner.run(appName="kub-dataset", forwardedArgs=["pull", "kernante"])

    assert exitCode == 0
    assert len(calls) == 2
    assert calls[1][:2] == ["/usr/bin/apptainer", "exec"]
    assert calls[1][2:] == [str(imagePath), "kub-dataset", "pull", "kernante"]
