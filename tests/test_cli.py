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
from kub_cli.wrapper_context import (
    ensureAutoMpiExecutionEnv,
    ensureSlurmAccountingEnv,
    exposeHostSlurmSupportFiles,
    resolveTargetSlurmPartition,
    selectClosestOpenMpiModule,
    selectAssociationForPartition,
)


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


def testSimulatePreflightFailsEarlyWhenLocalApptainerImageMissing(
    cliRunner: CliRunner,
    tmp_path: Path,
) -> None:
    fakeRunner = tmp_path / "apptainer"
    fakeRunner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fakeRunner.chmod(0o755)

    missingImage = tmp_path / "missing.sif"
    result = cliRunner.invoke(
        simulateApp,
        [
            "--runtime",
            "apptainer",
            "--runner",
            str(fakeRunner),
            "--image",
            str(missingImage),
            "status",
            "arz",
        ],
    )

    assert result.exit_code == 2
    assert "Preflight failed" in result.output
    assert "Container image not found" in result.output


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
    assert f"{tmp_path}/.kub-cli/host-bin:" in pathValues[0]
    assert "SBATCH_EXPORT=ALL" in envValues
    assert f"APPTAINER_CACHEDIR={tmp_path}/.kub-cli/apptainer/cache" in envValues
    assert f"APPTAINER_TMPDIR={tmp_path}/.kub-cli/apptainer/tmp" in envValues
    assert f"APPTAINER_CONFIGDIR={tmp_path}/.kub-cli/apptainer/config" in envValues
    assert f"SINGULARITY_CACHEDIR={tmp_path}/.kub-cli/apptainer/cache" in envValues
    assert f"SINGULARITY_TMPDIR={tmp_path}/.kub-cli/apptainer/tmp" in envValues
    assert f"SINGULARITY_CONFIGDIR={tmp_path}/.kub-cli/apptainer/config" in envValues

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


def testSimulateHostSlurmBridgeAddsSrunFallbackWhenMissing(
    cliRunner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("kub_cli.runtime.shutil.which", lambda _: "/usr/bin/docker")

    hostBin = tmp_path / "host-bin"
    hostBin.mkdir()
    hostSbatch = hostBin / "sbatch"
    hostSbatch.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    hostSbatch.chmod(0o755)

    def fakeWhich(name: str) -> str | None:
        if name == "sbatch":
            return str(hostSbatch)
        if name == "srun":
            return None
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
    assert f"{tmp_path}/.kub-cli/host-bin:" in pathValues[0]

    bridgeDir = tmp_path / ".kub-cli" / "host-bin"
    srunShim = bridgeDir / "srun"
    assert srunShim.exists()
    shimText = srunShim.read_text(encoding="utf-8")
    assert "mpirun" in shimText
    assert "mpiexec" in shimText
    assert "KUB_MPI_MODULES" in shimText
    assert "KUB_MPI_EXEC_MODE" in shimText


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


def testExposeHostSlurmSupportFilesDiscoversLinkedSlurmLibraries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    slurmBinDir = tmp_path / "slurm-bin"
    slurmLibDir = tmp_path / "slurm-lib"
    sbatchPath = slurmBinDir / "sbatch"
    srunPath = slurmBinDir / "srun"
    slurmLib = slurmLibDir / "libslurmfull.so"

    slurmBinDir.mkdir()
    slurmLibDir.mkdir()
    sbatchPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    srunPath.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    slurmLib.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_LIBRARY_DIR_CANDIDATES", ())
    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_CONFIG_DIR_CANDIDATES", ())
    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_IDENTITY_FILE_CANDIDATES", ())
    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_MUNGE_PATH_CANDIDATES", ())

    def fakeFindExecutable(commandName: str) -> str | None:
        if commandName == "sbatch":
            return str(sbatchPath)
        if commandName == "srun":
            return str(srunPath)
        return None

    class FakeProcess:
        def __init__(self, output: str, returnCode: int = 0) -> None:
            self._output = output
            self.returncode = returnCode

        def communicate(self):  # type: ignore[no-untyped-def]
            return self._output, ""

    def fakePopen(command, stdout=None, stderr=None, text=False):  # type: ignore[no-untyped-def]
        executable = command[1]
        if executable in {str(sbatchPath), str(srunPath)}:
            return FakeProcess(f"libslurmfull.so => {slurmLib} (0x0000)\n")
        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", fakeFindExecutable)
    monkeypatch.setattr("kub_cli.wrapper_context.subprocess.Popen", fakePopen)

    bindSpecs: list[str] = []
    envAssignments: list[str] = []
    exposeHostSlurmSupportFiles(
        bindSpecs=bindSpecs,
        envAssignments=envAssignments,
    )

    assert f"{slurmLibDir}:{slurmLibDir}" in bindSpecs
    assert any(
        assignment.startswith(f"LD_LIBRARY_PATH={slurmLibDir}")
        for assignment in envAssignments
    )


def testExposeHostSlurmSupportFilesEnablesSssIdentityLookup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    passwdFile = tmp_path / "passwd"
    groupFile = tmp_path / "group"
    nsswitchFile = tmp_path / "nsswitch.conf"
    sssDir = tmp_path / "sss"
    libDir = tmp_path / "lib64"

    passwdFile.write_text("root:x:0:0:root:/root:/bin/sh\n", encoding="utf-8")
    groupFile.write_text("root:x:0:\n", encoding="utf-8")
    nsswitchFile.write_text(
        "passwd: files sss\ngroup: files sss\n",
        encoding="utf-8",
    )
    sssDir.mkdir()
    libDir.mkdir()
    for libraryName in (
        "libnss_sss.so.2",
        "libsss_nss_idmap.so.0",
        "libsss_idmap.so.0",
    ):
        (libDir / libraryName).write_text("dummy", encoding="utf-8")

    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_LIBRARY_DIR_CANDIDATES", ())
    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_CONFIG_DIR_CANDIDATES", ())
    monkeypatch.setattr(
        "kub_cli.wrapper_context.SLURM_IDENTITY_FILE_CANDIDATES",
        (passwdFile, groupFile, nsswitchFile),
    )
    monkeypatch.setattr("kub_cli.wrapper_context.SLURM_MUNGE_PATH_CANDIDATES", ())
    monkeypatch.setattr(
        "kub_cli.wrapper_context.SSSD_RUNTIME_DIR_CANDIDATES",
        (sssDir,),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.NSS_LIBRARY_DIR_CANDIDATES",
        (libDir,),
    )
    monkeypatch.setattr("kub_cli.wrapper_context.findExecutable", lambda _: None)

    bindSpecs: list[str] = []
    envAssignments: list[str] = []
    exposeHostSlurmSupportFiles(
        bindSpecs=bindSpecs,
        envAssignments=envAssignments,
    )

    assert f"{sssDir}:{sssDir}" in bindSpecs
    assert f"{libDir / 'libnss_sss.so.2'}:{libDir / 'libnss_sss.so.2'}" in bindSpecs
    assert any(
        assignment.startswith(f"LD_LIBRARY_PATH={libDir}")
        for assignment in envAssignments
    )


def testResolveTargetSlurmPartitionFromProfileConfig(tmp_path: Path) -> None:
    configPath = tmp_path / ".kub-simulate.toml"
    configPath.write_text(
        "[profiles.apptainer-slurm]\n"
        'partition = "qcpu_long"\n',
        encoding="utf-8",
    )

    partition = resolveTargetSlurmPartition(
        forwardedArgs=("preprocess", "arz", "--profile", "apptainer-slurm"),
        hostCemdbRoot=tmp_path,
    )

    assert partition == "qcpu_long"


def testResolveTargetSlurmPartitionFallsBackToDefaults(tmp_path: Path) -> None:
    configPath = tmp_path / ".kub-simulate.toml"
    configPath.write_text(
        "[defaults]\n"
        'partition = "qcpu"\n',
        encoding="utf-8",
    )

    partition = resolveTargetSlurmPartition(
        forwardedArgs=("preprocess", "arz", "--profile", "missing-profile"),
        hostCemdbRoot=tmp_path,
    )

    assert partition == "qcpu"


def testSelectAssociationForPartitionParsesSacctmgrOutput() -> None:
    output = (
        "defac||normal\n"
        "eu-25-66|qcpu|3162_5315\n"
        "eu-25-66|qgpu|3162_5316\n"
    )

    association = selectAssociationForPartition(output=output, partition="qcpu")

    assert association == ("eu-25-66", "3162_5315")


def testEnsureSlurmAccountingEnvAddsDetectedAccountAndQos(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetSlurmPartition",
        lambda forwardedArgs, hostCemdbRoot: "qcpu",
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveSlurmAssociationForPartition",
        lambda partition: ("eu-25-66", "3162_5315"),
    )

    assignments: list[str] = []
    ensureSlurmAccountingEnv(
        envAssignments=assignments,
        forwardedArgs=("preprocess", "arz", "--profile", "apptainer-slurm"),
        hostCemdbRoot=tmp_path,
    )

    assert "SBATCH_ACCOUNT=eu-25-66" in assignments
    assert "SBATCH_QOS=3162_5315" in assignments


def testEnsureSlurmAccountingEnvDoesNotOverrideExplicitValues(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetSlurmPartition",
        lambda forwardedArgs, hostCemdbRoot: "qcpu",
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveSlurmAssociationForPartition",
        lambda partition: ("eu-25-66", "3162_5315"),
    )

    assignments = ["SBATCH_ACCOUNT=manual-account", "SBATCH_QOS=manual-qos"]
    ensureSlurmAccountingEnv(
        envAssignments=assignments,
        forwardedArgs=("preprocess", "arz", "--profile", "apptainer-slurm"),
        hostCemdbRoot=tmp_path,
    )

    assert "SBATCH_ACCOUNT=manual-account" in assignments
    assert "SBATCH_QOS=manual-qos" in assignments


def testEnsureSlurmAccountingEnvWithCustomConfigAndExplicitPartition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetSlurmPartition",
        lambda forwardedArgs, hostCemdbRoot: "qcpu",
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveSlurmAssociationForPartition",
        lambda partition: ("eu-25-66", "3162_5315"),
    )

    assignments: list[str] = []
    ensureSlurmAccountingEnv(
        envAssignments=assignments,
        forwardedArgs=(
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--config",
            "/tmp/custom.toml",
            "--partition",
            "qcpu",
        ),
        hostCemdbRoot=tmp_path,
    )

    assert "SBATCH_ACCOUNT=eu-25-66" in assignments
    assert "SBATCH_QOS=3162_5315" in assignments


def testEnsureSlurmAccountingEnvSkipsCustomConfigWithoutExplicitPartition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetSlurmPartition",
        lambda forwardedArgs, hostCemdbRoot: (_ for _ in ()).throw(
            AssertionError("partition detection should be skipped")
        ),
    )

    assignments: list[str] = []
    ensureSlurmAccountingEnv(
        envAssignments=assignments,
        forwardedArgs=(
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
            "--config",
            "/tmp/custom.toml",
        ),
        hostCemdbRoot=tmp_path,
    )

    assert assignments == []


def testEnsureSlurmAccountingEnvSkipsNonSlurmInvocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetSlurmPartition",
        lambda forwardedArgs, hostCemdbRoot: (_ for _ in ()).throw(
            AssertionError("partition detection should be skipped")
        ),
    )

    assignments: list[str] = []
    ensureSlurmAccountingEnv(
        envAssignments=assignments,
        forwardedArgs=("run", "arz", "--launcher", "local"),
        hostCemdbRoot=tmp_path,
    )

    assert assignments == []


def testSelectClosestOpenMpiModulePrefersExactVersion() -> None:
    selected = selectClosestOpenMpiModule(
        moduleCandidates=(
            "OpenMPI/4.1.5-GCC-12.3.0",
            "OpenMPI/4.1.6-GCC-13.2.0",
            "OpenMPI/4.1.8-GCC-13.2.0",
        ),
        targetVersion="4.1.6",
    )

    assert selected == "OpenMPI/4.1.6-GCC-13.2.0"


def testSelectClosestOpenMpiModuleFallsBackToNearestPatch() -> None:
    selected = selectClosestOpenMpiModule(
        moduleCandidates=(
            "OpenMPI/4.1.5-GCC-12.3.0",
            "OpenMPI/4.1.8-GCC-13.2.0",
        ),
        targetVersion="4.1.6",
    )

    assert selected == "OpenMPI/4.1.5-GCC-12.3.0"


def testEnsureAutoMpiExecutionEnvInjectsModeAndModule(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub-master.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr(
        "kub_cli.wrapper_context.shouldAttemptAutoMpiDetection",
        lambda forwardedArgs, hostCemdbRoot: True,
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetApptainerImagePath",
        lambda forwardedArgs, hostCemdbRoot, runtimeCwd: imagePath,
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveHostApptainerExecutablePath",
        lambda configHint=None: Path("/usr/bin/apptainer"),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.detectImageOpenMpiVersion",
        lambda apptainerExecutable, imagePath: "4.1.6",
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.discoverAvailableOpenMpiModules",
        lambda: (
            "OpenMPI/4.1.5-GCC-12.3.0",
            "OpenMPI/4.1.6-GCC-13.2.0",
        ),
    )

    assignments: list[str] = []
    ensureAutoMpiExecutionEnv(
        envAssignments=assignments,
        forwardedArgs=("preprocess", "arz", "--profile", "apptainer-slurm"),
        hostCemdbRoot=tmp_path,
        runtimeCwd=tmp_path,
        configHint=None,
    )

    assert "KUB_MPI_MODULES=OpenMPI/4.1.6-GCC-13.2.0" in assignments
    assert "KUB_MPI_EXEC_MODE=prefer-mpi" in assignments


def testEnsureAutoMpiExecutionEnvDoesNotOverrideExplicitModeOrModules(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imagePath = tmp_path / "kub-master.sif"
    imagePath.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr(
        "kub_cli.wrapper_context.shouldAttemptAutoMpiDetection",
        lambda forwardedArgs, hostCemdbRoot: True,
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveTargetApptainerImagePath",
        lambda forwardedArgs, hostCemdbRoot, runtimeCwd: imagePath,
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.resolveHostApptainerExecutablePath",
        lambda configHint=None: Path("/usr/bin/apptainer"),
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.detectImageOpenMpiVersion",
        lambda apptainerExecutable, imagePath: "4.1.6",
    )
    monkeypatch.setattr(
        "kub_cli.wrapper_context.discoverAvailableOpenMpiModules",
        lambda: ("OpenMPI/4.1.6-GCC-13.2.0",),
    )

    assignments = [
        "KUB_MPI_MODULES=manual-openmpi",
        "KUB_MPI_EXEC_MODE=auto",
    ]
    ensureAutoMpiExecutionEnv(
        envAssignments=assignments,
        forwardedArgs=("preprocess", "arz", "--profile", "apptainer-slurm"),
        hostCemdbRoot=tmp_path,
        runtimeCwd=tmp_path,
        configHint=None,
    )

    assert "KUB_MPI_MODULES=manual-openmpi" in assignments
    assert "KUB_MPI_EXEC_MODE=auto" in assignments


def testSimulateApptainerStorageEnvRespectsExplicitOverrides(
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
            "--env",
            "APPTAINER_CACHEDIR=/custom/cache",
            "--env",
            "APPTAINER_TMPDIR=/custom/tmp",
            "--env",
            "APPTAINER_CONFIGDIR=/custom/config",
            "preprocess",
            "arz",
            "--profile",
            "apptainer-slurm",
        ],
    )

    assert result.exit_code == 0
    command = captured["command"]  # type: ignore[assignment]
    envValues = extractFlagValues(command, "--env")
    assert "APPTAINER_CACHEDIR=/custom/cache" in envValues
    assert "APPTAINER_TMPDIR=/custom/tmp" in envValues
    assert "APPTAINER_CONFIGDIR=/custom/config" in envValues
    assert "SINGULARITY_CACHEDIR=/custom/cache" in envValues
    assert "SINGULARITY_TMPDIR=/custom/tmp" in envValues
    assert "SINGULARITY_CONFIGDIR=/custom/config" in envValues
