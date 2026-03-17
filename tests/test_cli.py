# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest
from typer.testing import CliRunner

from kub_cli.cli import dashboardApp, datasetApp, simulateApp
from kub_cli.wrapper_context import exposeHostSlurmSupportFiles


@pytest.fixture
def cliRunner() -> CliRunner:
    return CliRunner()


def extractFlagValues(command: list[str], flag: str) -> list[str]:
    values: list[str] = []
    for index, token in enumerate(command):
        if token == flag and index + 1 < len(command):
            values.append(command[index + 1])
    return values


def testForwardingArgsWithDoubleDashApptainer(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--image",
            str(imagePath),
            "--runner",
            "apptainer",
            "--",
            "run",
            "case.yaml",
            "--mesh",
            "fine",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[-4:] == ["run", "case.yaml", "--mesh", "fine"]
    assert "--cemdb-root" not in command
    bindValues = extractFlagValues(command, "--bind")
    assert any(value.endswith(":/cemdb") for value in bindValues)
    pwdValues = extractFlagValues(command, "--pwd")
    assert "/cemdb" in pwdValues


def testForwardingArgsWithoutDoubleDashDocker(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "push",
            "./data",
            "--tag",
            "baseline",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[0] == "/usr/bin/docker"
    assert command[1] == "run"
    assert command[-6:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-dataset",
        "push",
        "./data",
        "--tag",
        "baseline",
    ]
    volumeValues = extractFlagValues(command, "--volume")
    assert any(value.endswith(":/cemdb") for value in volumeValues)
    envValues = extractFlagValues(command, "--env")
    assert "HOME=/cemdb" in envValues
    assert "KUB_CONFIG=/cemdb/.kub/config.toml" in envValues
    workdirValues = extractFlagValues(command, "--workdir")
    assert "/cemdb" in workdirValues


def testBindOptionIsPassedToDockerAsVolume(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--bind",
            "/data:/data",
            "--bind",
            "/scratch:/scratch",
            "push",
            "/data/myset",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert "--volume" in command
    assert "/data:/data" in command
    assert "/scratch:/scratch" in command
    assert any(value.endswith(":/cemdb") for value in extractFlagValues(command, "--volume"))


def testDryRunPrintsDockerCommandAndSkipsExecution(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        dashboardApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--dry-run",
            "serve",
            "./results",
        ],
    )

    assert result.exit_code == 0
    assert called["value"] is False
    assert "docker run" in result.stdout
    assert "kub-dashboard serve ./results" in result.stdout


def testSelectedDockerRuntimeWithoutRunnerIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: None)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
        ],
    )

    assert result.exit_code == 2
    assert "unable to find docker runner" in result.output.lower()


def testSelectedApptainerRuntimeMissingImageIsReported(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingImage = tmp_path / "missing.sif"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/apptainer")

    result = cliRunner.invoke(
        datasetApp,
        ["--runtime", "apptainer", "--image", str(missingImage), "push", "./data"],
    )

    assert result.exit_code == 2
    assert "Container image not found" in result.output


def testAutoRuntimeFallsBackToDocker(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingImage = tmp_path / "missing.sif"

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "auto",
            "--image",
            str(missingImage),
            "--",
            "run",
            "case.yaml",
        ],
        env={"KUB_IMAGE_DOCKER": "ghcr.io/feelpp/ktirio-urban-building:master"},
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    assert command[0] == "/usr/bin/docker"


def testInnerVersionOptionIsForwardedAfterFirstArgument(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "cemdb").mkdir()

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "pull-simulator",
            "--version",
            "0.2.0",
            "--cemdb-root",
            "cemdb",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "kub-cli 0." not in result.stdout

    command = captured["command"]  # type: ignore[assignment]
    assert command[-6:] == [
        "pull-simulator",
        "--version",
        "0.2.0",
        "--cemdb-root",
        "/cemdb",
        "--force",
    ]
    volumeValues = extractFlagValues(command, "--volume")
    assert f"{(tmp_path / 'cemdb').resolve()}:/cemdb" in volumeValues


def testExplicitWrapperCemdbRootIsMountedAndForwarded(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hostCemdb = tmp_path / "my-cemdb"
    hostCemdb.mkdir()

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(hostCemdb),
            "run",
            "case.yaml",
        ],
    )

    assert result.exit_code == 0

    command = captured["command"]  # type: ignore[assignment]
    volumeValues = extractFlagValues(command, "--volume")
    assert f"{hostCemdb.resolve()}:/cemdb" in volumeValues
    assert command[-4:] == [
        "--config",
        "/cemdb/.kub-simulate.toml",
        "run",
        "case.yaml",
    ]


def testMissingWrapperCemdbRootIsCreatedAndUsed(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missingRoot = tmp_path / "missing-cemdb"

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(missingRoot),
            "push",
            "./data",
        ],
    )

    assert result.exit_code == 0
    assert missingRoot.exists()
    assert missingRoot.is_dir()
    assert (missingRoot / ".kub").is_dir()

    command = captured["command"]  # type: ignore[assignment]
    volumeValues = extractFlagValues(command, "--volume")
    assert f"{missingRoot.resolve()}:/cemdb" in volumeValues


def testShowConfigIsReadOnlyAndDoesNotCreateCemdbRoot(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    missingRoot = tmp_path / "missing-cemdb"
    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--show-config",
            "--cemdb-root",
            str(missingRoot),
        ],
    )

    assert result.exit_code == 0
    assert called["value"] is False
    assert not missingRoot.exists()
    assert '"runtime": "docker"' in result.output


def testShowConfigSkipsExecutionEvenWithForwardedArguments(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    called = {"value": False}

    def fakeRun(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["value"] = True
        return subprocess.CompletedProcess(args=[], returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--show-config",
            "push",
            "./data",
        ],
    )

    assert result.exit_code == 0
    assert called["value"] is False
    assert '"runtime": "docker"' in result.output


def testExplicitEnvOverridesDefaultCemdbEnv(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hostCemdb = tmp_path / "cemdb"
    hostCemdb.mkdir()

    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        datasetApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--cemdb-root",
            str(hostCemdb),
            "--env",
            "HOME=/tmp/custom-home",
            "--env",
            "KUB_CONFIG=/tmp/custom-kub.toml",
            "pull-simulator",
            "--version",
            "0.2.0",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    envValues = extractFlagValues(command, "--env")
    assert "HOME=/tmp/custom-home" in envValues
    assert "KUB_CONFIG=/tmp/custom-kub.toml" in envValues


def testSimulatePreprocessArgumentsAreForwardedUnchanged(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "preprocess",
            "arz",
            "--version",
            "0.1.0",
            "--profile",
            "apptainer-slurm",
            "--partitions",
            "32",
            "64",
            "128",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    imageIndex = command.index("ghcr.io/feelpp/ktirio-urban-building:master")
    assert command[imageIndex:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-simulate",
        "--config",
        "/cemdb/.kub-simulate.toml",
        "preprocess",
        "arz",
        "--version",
        "0.1.0",
        "--profile",
        "apptainer-slurm",
        "--partitions",
        "32",
        "64",
        "128",
        "--dry-run",
    ]


def testSimulateInnerRuntimeOptionIsForwarded(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "run",
            "arz",
            "--runtime",
            "apptainer",
            "--apptainer-image",
            "/cemdb/sim.sif",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    imageIndex = command.index("ghcr.io/feelpp/ktirio-urban-building:master")
    assert command[imageIndex:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-simulate",
        "--config",
        "/cemdb/.kub-simulate.toml",
        "run",
        "arz",
        "--runtime",
        "apptainer",
        "--apptainer-image",
        "/cemdb/sim.sif",
        "--dry-run",
    ]


def testSimulateConfigSubcommandIsForwarded(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "config",
            "show",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    imageIndex = command.index("ghcr.io/feelpp/ktirio-urban-building:master")
    assert command[imageIndex:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-simulate",
        "--config",
        "/cemdb/.kub-simulate.toml",
        "config",
        "show",
    ]


def testSimulateExplicitConfigOptionOverridesWrapperDefault(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "--",
            "--config",
            "/cemdb/custom.toml",
            "status",
            "arz",
            "--last",
            "5",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    imageIndex = command.index("ghcr.io/feelpp/ktirio-urban-building:master")
    assert command[imageIndex:] == [
        "ghcr.io/feelpp/ktirio-urban-building:master",
        "kub-simulate",
        "--config",
        "/cemdb/custom.toml",
        "status",
        "arz",
        "--last",
        "5",
    ]


def testSimulateConfigIsMirroredToNestedCemdbFile(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "cemdb").mkdir()
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    rootConfig = tmp_path / ".kub-simulate.toml"
    nestedConfig = tmp_path / "cemdb" / ".kub-simulate.toml"

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        rootConfig.write_text(
            "# kub-simulate user configuration\n\n"
            "[defaults]\n"
            "cemdb_root = \"cemdb/locations\"\n\n"
            "[profiles.apptainer-slurm]\n"
            "partition = \"public\"\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "config",
            "set",
            "partition",
            "public",
            "--profile-target",
            "apptainer-slurm",
        ],
    )

    assert result.exit_code == 0
    assert rootConfig.exists()
    assert nestedConfig.exists()
    assert "partition = \"public\"" in nestedConfig.read_text(encoding="utf-8")


def testSimulateRootConfigIsInitializedFromNestedFileWhenMissing(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    nestedDir = tmp_path / "cemdb"
    nestedDir.mkdir()
    nestedConfig = nestedDir / ".kub-simulate.toml"
    nestedConfig.write_text(
        "# kub-simulate user configuration\n\n[defaults]\ncemdb_root = \"cemdb/locations\"\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "status",
            "arz",
            "--last",
            "5",
        ],
    )

    assert result.exit_code == 0
    rootConfig = tmp_path / ".kub-simulate.toml"
    assert rootConfig.exists()
    assert "cemdb_root = \"cemdb/locations\"" in rootConfig.read_text(encoding="utf-8")


def testSimulateDryRunInjectsSlurmShimsInPath(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", lambda _: None)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    envValues = extractFlagValues(command, "--env")
    pathValues = [value for value in envValues if value.startswith("PATH=")]
    assert len(pathValues) == 1
    assert pathValues[0].startswith("PATH=/cemdb/.kub-cli/shims:")

    shimDir = tmp_path / ".kub-cli" / "shims"
    assert (shimDir / "sbatch").exists()
    assert (shimDir / "srun").exists()


def testSimulateNonDryRunInjectsHostSlurmBridgeWhenAvailable(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    hostBin = tmp_path / "host-bin"
    hostBin.mkdir()
    hostSbatch = hostBin / "sbatch"
    hostSrun = hostBin / "srun"
    hostSbatch.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    hostSrun.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    hostSbatch.chmod(0o755)
    hostSrun.chmod(0o755)

    def fakeWhich(name: str) -> str | None:
        if name == "sbatch":
            return str(hostSbatch)
        if name == "srun":
            return str(hostSrun)
        return None

    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", fakeWhich)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    envValues = extractFlagValues(command, "--env")
    pathValues = [value for value in envValues if value.startswith("PATH=")]
    assert len(pathValues) == 1
    assert pathValues[0].startswith("PATH=/cemdb/.kub-cli/shims:")
    assert "/cemdb/.kub-cli/host-bin:" in pathValues[0]

    bridgeDir = tmp_path / ".kub-cli" / "host-bin"
    assert (bridgeDir / "sbatch").exists()
    assert (bridgeDir / "srun").exists()


def testHostSlurmBridgeSkipsSameFileCopy(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    bridgeDir = tmp_path / ".kub-cli" / "host-bin"
    bridgeDir.mkdir(parents=True)

    localSbatch = bridgeDir / "sbatch"
    localSrun = bridgeDir / "srun"
    localSbatch.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    localSrun.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    localSbatch.chmod(0o755)
    localSrun.chmod(0o755)

    def fakeFindExecutable(name: str) -> str | None:
        if name == "sbatch":
            return str(localSbatch)
        if name == "srun":
            return str(localSrun)
        return None

    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", fakeFindExecutable)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
        ],
    )

    assert result.exit_code == 0


def testSimulatePreprocessInjectsSlurmShimsWhenHostSlurmMissing(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", lambda _: None)

    captured: dict[str, object] = {}

    def fakeRun(command, check, env):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "docker",
            "--image",
            "ghcr.io/feelpp/ktirio-urban-building:master",
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--partitions",
            "32",
            "64",
            "128",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    envValues = extractFlagValues(command, "--env")
    pathValues = [value for value in envValues if value.startswith("PATH=")]
    assert len(pathValues) == 1
    assert pathValues[0] == (
        "PATH=/cemdb/.kub-cli/shims:"
        "/opt/kub-venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )

    shimDir = tmp_path / ".kub-cli" / "shims"
    assert (shimDir / "sbatch").exists()
    assert (shimDir / "srun").exists()


def testSimulateApptainerProfileExposesHostApptainerExecutable(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    runnerPath = tmp_path / "opt" / "apptainer" / "latest" / "bin" / "apptainer"
    runnerPath.parent.mkdir(parents=True)
    runnerPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runnerPath.chmod(0o755)

    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", lambda _: None)

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
        captured["env"] = env
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--runner",
            str(runnerPath),
            "--image",
            str(imagePath),
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--partitions",
            "32",
            "64",
            "128",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    bindValues = extractFlagValues(command, "--bind")
    runnerDir = str(runnerPath.parent)
    assert f"{runnerDir}:{runnerDir}" in bindValues

    executionEnv = captured["env"]  # type: ignore[assignment]
    assert isinstance(executionEnv, dict)
    pathValue = executionEnv["PATH"]  # type: ignore[index]
    assert pathValue.startswith(f"{runnerDir}:/cemdb/.kub-cli/shims:")


def testSimulateSlurmProfileDefaultsToHostWorkdirAndHostBind(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    runnerPath = tmp_path / "opt" / "apptainer" / "latest" / "bin" / "apptainer"
    runnerPath.parent.mkdir(parents=True)
    runnerPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runnerPath.chmod(0o755)

    imagePath = tmp_path / "kub.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", lambda _: None)

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
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.runtime.subprocess.run", fakeRun)

    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--runner",
            str(runnerPath),
            "--image",
            str(imagePath),
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--partitions",
            "32",
            "64",
            "128",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    bindValues = extractFlagValues(command, "--bind")
    assert f"{tmp_path}:{tmp_path}" in bindValues
    pwdValues = extractFlagValues(command, "--pwd")
    assert str(tmp_path) in pwdValues


def testExposeHostSlurmSupportFilesIncludesIdentityFiles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    slurmLibDir = tmp_path / "lib-slurm"
    slurmCfgDir = tmp_path / "etc-slurm"
    passwdFile = tmp_path / "passwd"
    groupFile = tmp_path / "group"
    nsswitchFile = tmp_path / "nsswitch.conf"
    mungeDir = tmp_path / "run-munge"

    slurmLibDir.mkdir()
    slurmCfgDir.mkdir()
    mungeDir.mkdir()
    passwdFile.write_text("root:x:0:0:root:/root:/bin/sh\n", encoding="utf-8")
    groupFile.write_text("root:x:0:\n", encoding="utf-8")
    nsswitchFile.write_text("passwd: files\n", encoding="utf-8")

    monkeypatch.setattr(
        "kub_cli.wrapper_context.SLURM_LIBRARY_DIR_CANDIDATES",
        (slurmLibDir,),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.SLURM_CONFIG_DIR_CANDIDATES",
        (slurmCfgDir,),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.SLURM_IDENTITY_FILE_CANDIDATES",
        (passwdFile, groupFile, nsswitchFile),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.SLURM_MUNGE_PATH_CANDIDATES",
        (mungeDir,),
    )

    bindSpecs: list[str] = []
    exposeHostSlurmSupportFiles(bindSpecs=bindSpecs)

    assert f"{slurmLibDir}:{slurmLibDir}" in bindSpecs
    assert f"{slurmCfgDir}:{slurmCfgDir}" in bindSpecs
    assert f"{passwdFile}:{passwdFile}" in bindSpecs
    assert f"{groupFile}:{groupFile}" in bindSpecs
    assert f"{nsswitchFile}:{nsswitchFile}" in bindSpecs
    assert f"{mungeDir}:{mungeDir}" in bindSpecs
