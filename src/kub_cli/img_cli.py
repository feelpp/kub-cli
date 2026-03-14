# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Typer CLI for kub-img image management."""

from __future__ import annotations

import json

import typer

from . import __version__
from .errors import KubCliError
from .logging_utils import configureLogging
from .img_tools import KubImgManager, resolveImgConfig


imgApp = typer.Typer(
    add_completion=False,
    help="Manage kub runtime images for Apptainer and Docker.",
)


def buildImgManager(
    *,
    runtime: str | None,
    image: str | None,
    runner: str | None,
    verbose: bool | None,
    showConfig: bool,
) -> KubImgManager:
    config = resolveImgConfig(
        runtime=runtime,
        image=image,
        runner=runner,
        verbose=verbose,
    )
    configureLogging(config.verbose)

    if showConfig:
        print(json.dumps(config.toDict(), indent=2, sort_keys=True))

    return KubImgManager(config=config)


def exitOnError(error: KubCliError) -> None:
    typer.secho(str(error), fg=typer.colors.RED, err=True)
    raise typer.Exit(code=error.exit_code) from error


@imgApp.callback(invoke_without_command=True)
def root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        is_eager=True,
        help="Show kub-cli version and exit.",
    ),
) -> None:
    if version:
        typer.echo(f"kub-cli {__version__}")
        raise typer.Exit(code=0)

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(code=0)


@imgApp.command("pull")
def pullImageCommand(
    source: str | None = typer.Argument(
        None,
        metavar="SOURCE",
        help=(
            "Optional pull source. "
            "For Apptainer use oras://... (never docker://). "
            "If omitted, source is derived from configuration."
        ),
    ),
    runtime: str | None = typer.Option(
        None,
        "--runtime",
        metavar="{auto,apptainer,docker}",
        help="Container runtime selection.",
    ),
    image: str | None = typer.Option(
        None,
        "--image",
        metavar="IMAGE",
        help="Runtime image path/reference (destination for pull).",
    ),
    runner: str | None = typer.Option(
        None,
        "--runner",
        metavar="PATH",
        help="Runtime executable path or command name.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite destination image when runtime is Apptainer.",
    ),
    disableCache: bool = typer.Option(
        False,
        "--disable-cache",
        help="Disable cache when runtime is Apptainer.",
    ),
    apptainerFlags: list[str] | None = typer.Option(
        None,
        "--apptainer-flag",
        metavar="FLAG",
        help="Additional apptainer pull flags (repeatable).",
    ),
    dockerFlags: list[str] | None = typer.Option(
        None,
        "--docker-flag",
        metavar="FLAG",
        help="Additional docker pull flags (repeatable).",
    ),
    dryRun: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the pull command without executing it.",
    ),
    verbose: bool | None = typer.Option(
        None,
        "--verbose/--no-verbose",
        help="Enable or disable verbose logging.",
    ),
    showConfig: bool = typer.Option(
        False,
        "--show-config",
        help="Print effective kub-cli configuration as JSON.",
    ),
) -> None:
    try:
        manager = buildImgManager(
            runtime=runtime,
            image=image,
            runner=runner,
            verbose=verbose,
            showConfig=showConfig,
        )
        exitCode = manager.pullImage(
            runtime=runtime,
            source=source,
            force=force,
            disableCache=disableCache,
            apptainerFlags=apptainerFlags or [],
            dockerFlags=dockerFlags or [],
            dryRun=dryRun,
        )
    except KubCliError as error:
        exitOnError(error)

    raise typer.Exit(code=exitCode)


@imgApp.command("info")
def infoCommand(
    runtime: str | None = typer.Option(
        None,
        "--runtime",
        metavar="{auto,apptainer,docker}",
        help="Container runtime selection.",
    ),
    image: str | None = typer.Option(
        None,
        "--image",
        metavar="IMAGE",
        help="Runtime image path/reference.",
    ),
    runner: str | None = typer.Option(
        None,
        "--runner",
        metavar="PATH",
        help="Runtime executable path or command name.",
    ),
    jsonOutput: bool = typer.Option(
        False,
        "--json",
        help="Emit image info as JSON.",
    ),
    verbose: bool | None = typer.Option(
        None,
        "--verbose/--no-verbose",
        help="Enable or disable verbose logging.",
    ),
    showConfig: bool = typer.Option(
        False,
        "--show-config",
        help="Print effective kub-cli configuration as JSON.",
    ),
) -> None:
    try:
        manager = buildImgManager(
            runtime=runtime,
            image=image,
            runner=runner,
            verbose=verbose,
            showConfig=showConfig,
        )
        exitCode = manager.printInfo(runtime=runtime, jsonOutput=jsonOutput)
    except KubCliError as error:
        exitOnError(error)

    raise typer.Exit(code=exitCode)


@imgApp.command("apps")
def appsCommand(
    runtime: str | None = typer.Option(
        "apptainer",
        "--runtime",
        metavar="{apptainer}",
        help="Runtime selection for app listing (Apptainer only).",
    ),
    image: str | None = typer.Option(
        None,
        "--image",
        metavar="IMAGE",
        help="Apptainer image path.",
    ),
    runner: str | None = typer.Option(
        None,
        "--runner",
        metavar="PATH",
        help="Apptainer executable path or command name.",
    ),
    verbose: bool | None = typer.Option(
        None,
        "--verbose/--no-verbose",
        help="Enable or disable verbose logging.",
    ),
    showConfig: bool = typer.Option(
        False,
        "--show-config",
        help="Print effective kub-cli configuration as JSON.",
    ),
) -> None:
    try:
        manager = buildImgManager(
            runtime=runtime,
            image=image,
            runner=runner,
            verbose=verbose,
            showConfig=showConfig,
        )
        exitCode = manager.printApps(runtime=runtime)
    except KubCliError as error:
        exitOnError(error)

    raise typer.Exit(code=exitCode)


@imgApp.command("path")
def pathCommand(
    runtime: str | None = typer.Option(
        None,
        "--runtime",
        metavar="{auto,apptainer,docker}",
        help="Runtime selection for resolved image path/reference.",
    ),
    image: str | None = typer.Option(
        None,
        "--image",
        metavar="IMAGE",
        help="Image path/reference override.",
    ),
    runner: str | None = typer.Option(
        None,
        "--runner",
        metavar="PATH",
        help="Runtime executable path or command name.",
    ),
    verbose: bool | None = typer.Option(
        None,
        "--verbose/--no-verbose",
        help="Enable or disable verbose logging.",
    ),
    showConfig: bool = typer.Option(
        False,
        "--show-config",
        help="Print effective kub-cli configuration as JSON.",
    ),
) -> None:
    try:
        manager = buildImgManager(
            runtime=runtime,
            image=image,
            runner=runner,
            verbose=verbose,
            showConfig=showConfig,
        )
        exitCode = manager.printImagePath(runtime=runtime)
    except KubCliError as error:
        exitOnError(error)

    raise typer.Exit(code=exitCode)


def imgMain() -> None:
    imgApp(prog_name="kub-img")
