# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from kub_cli.config import KubConfig
from kub_cli.errors import KubCliError
from kub_cli.img_integration import (
    KubImgCommandRunner,
    buildKubImgInfoRequest,
    buildKubImgPullRequest,
)


def testBuildPullRequestDerivesOrasFromDockerImage() -> None:
    config = KubConfig(
        runtime="apptainer",
        imageApptainer="/tmp/kub.sif",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    request = buildKubImgPullRequest(config)

    assert request.runtime == "apptainer"
    assert request.image == "/tmp/kub.sif"
    assert request.source == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"


def testBuildPullRequestUsesExplicitApptainerOrasSource() -> None:
    config = KubConfig(
        runtime="apptainer",
        image="/tmp/kub.sif",
        imageApptainer="oras://ghcr.io/feelpp/ktirio-urban-building:master-sif",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    request = buildKubImgPullRequest(config)

    assert request.runtime == "apptainer"
    assert request.image == "/tmp/kub.sif"
    assert request.source == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"


def testBuildPullRequestFallsBackToDefaultApptainerImagePath(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    config = KubConfig(runtime="apptainer")

    request = buildKubImgPullRequest(config)

    assert request.runtime == "apptainer"
    assert request.source == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"
    assert request.image == str((tmp_path / "kub-master.sif").resolve())


def testBuildPullRequestAutoPrefersApptainerWhenAvailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    def fakeWhich(name: str) -> str | None:
        if name == "apptainer":
            return "/usr/bin/apptainer"
        if name == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr("kub_cli.runtime.shutil.which", fakeWhich)

    request = buildKubImgPullRequest(KubConfig(runtime="auto"))

    assert request.runtime == "apptainer"
    assert request.source == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"
    assert request.image == str((tmp_path / "kub-master.sif").resolve())


def testBuildPullRequestIgnoresLocalImageOverrideForSourceDerivation() -> None:
    config = KubConfig(
        runtime="apptainer",
        imageOverride="/tmp/custom.sif",
    )

    request = buildKubImgPullRequest(config)

    assert request.runtime == "apptainer"
    assert request.image == "/tmp/custom.sif"
    assert request.source == "oras://ghcr.io/feelpp/ktirio-urban-building:master-sif"


def testBuildInfoRequestDocker() -> None:
    config = KubConfig(
        runtime="docker",
        imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
    )

    request = buildKubImgInfoRequest(config)

    assert request.runtime == "docker"
    assert request.image == "ghcr.io/feelpp/ktirio-urban-building:master"


def testKubImgCommandRunnerPullInvocation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kub_cli.img_integration.shutil.which", lambda _: "/usr/bin/kub-img")

    captured: dict[str, object] = {}

    def fakeRun(command, check):  # type: ignore[no-untyped-def]
        captured["command"] = command
        captured["check"] = check
        return subprocess.CompletedProcess(args=command, returncode=0)

    monkeypatch.setattr("kub_cli.img_integration.subprocess.run", fakeRun)

    runner = KubImgCommandRunner(verbose=True)
    exitCode = runner.pullImage(
        request=buildKubImgPullRequest(
            KubConfig(
                runtime="docker",
                imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
            )
        ),
        dryRun=False,
    )

    assert exitCode == 0
    assert captured["check"] is False
    assert captured["command"][:4] == ["/usr/bin/kub-img", "pull", "--runtime", "docker"]


def testKubImgCommandRunnerInfoInvocation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kub_cli.img_integration.shutil.which", lambda _: "/usr/bin/kub-img")

    def fakeRun(command, check, capture_output, text):  # type: ignore[no-untyped-def]
        assert capture_output is True
        assert text is True
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout='{"runtime":"docker","image":"ghcr.io/x/y:tag"}',
            stderr="",
        )

    monkeypatch.setattr("kub_cli.img_integration.subprocess.run", fakeRun)

    runner = KubImgCommandRunner()
    payload = runner.inspectImageInfo(
        buildKubImgInfoRequest(
            KubConfig(runtime="docker", imageDocker="ghcr.io/x/y:tag")
        )
    )

    assert payload["runtime"] == "docker"


def testKubImgCommandRunnerFailsOnNonZero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kub_cli.img_integration.shutil.which", lambda _: "/usr/bin/kub-img")

    def fakeRun(command, check):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(args=command, returncode=9)

    monkeypatch.setattr("kub_cli.img_integration.subprocess.run", fakeRun)

    runner = KubImgCommandRunner()

    with pytest.raises(KubCliError):
        runner.pullImage(
            request=buildKubImgPullRequest(
                KubConfig(
                    runtime="docker",
                    imageDocker="ghcr.io/feelpp/ktirio-urban-building:master",
                )
            )
        )
