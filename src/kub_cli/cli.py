# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Typer CLI entrypoints for kub-cli wrapper commands."""

from __future__ import annotations

from typing import Sequence

import typer

from . import __version__
from .commands import WrapperOptions, runWrapperCommand
from .errors import KubCliError


CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}


def normalizeForwardedArgs(args: Sequence[str]) -> list[str]:
    forwarded = list(args)
    if forwarded and forwarded[0] == "--":
        return forwarded[1:]
    return forwarded


def executeWrapperCommand(
    *,
    appName: str,
    ctx: typer.Context,
    runtime: str | None,
    image: str | None,
    bind: Sequence[str],
    pwd: str | None,
    runner: str | None,
    dryRun: bool,
    verbose: bool | None,
    apptainerFlags: Sequence[str],
    dockerFlags: Sequence[str],
    envVars: Sequence[str],
    showConfig: bool,
) -> None:
    forwardedArgs = normalizeForwardedArgs(ctx.args)
    options = WrapperOptions(
        runtime=runtime,
        image=image,
        binds=tuple(bind),
        pwd=pwd,
        runner=runner,
        dryRun=dryRun,
        verbose=verbose,
        apptainerFlags=tuple(apptainerFlags),
        dockerFlags=tuple(dockerFlags),
        envVars=tuple(envVars),
        showConfig=showConfig,
    )

    try:
        exitCode = runWrapperCommand(
            appName=appName,
            forwardedArgs=forwardedArgs,
            options=options,
        )
    except KubCliError as error:
        typer.secho(str(error), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=error.exit_code) from error

    raise typer.Exit(code=exitCode)


def createWrapperApp(*, appName: str, helpText: str) -> typer.Typer:
    app = typer.Typer(
        add_completion=False,
        no_args_is_help=False,
        help=helpText,
    )

    @app.command(context_settings=CONTEXT_SETTINGS)
    def wrapper(
        ctx: typer.Context,
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
            help="Runtime image reference/path (runtime-dependent).",
        ),
        bind: list[str] | None = typer.Option(
            None,
            "--bind",
            metavar="SRC:DST",
            help="Bind mount or volume mapping (repeatable).",
        ),
        pwd: str | None = typer.Option(
            None,
            "--pwd",
            metavar="PATH",
            help="Working directory passed to runtime command.",
        ),
        runner: str | None = typer.Option(
            None,
            "--runner",
            metavar="PATH",
            help="Runtime executable path or command name.",
        ),
        dryRun: bool = typer.Option(
            False,
            "--dry-run",
            help="Print the resolved command without running it.",
        ),
        verbose: bool | None = typer.Option(
            None,
            "--verbose/--no-verbose",
            help="Enable or disable verbose wrapper logs.",
        ),
        apptainerFlags: list[str] | None = typer.Option(
            None,
            "--apptainer-flag",
            metavar="FLAG",
            help="Extra Apptainer flag (repeatable).",
        ),
        dockerFlags: list[str] | None = typer.Option(
            None,
            "--docker-flag",
            metavar="FLAG",
            help="Extra Docker flag (repeatable).",
        ),
        envVars: list[str] | None = typer.Option(
            None,
            "--env",
            metavar="KEY=VALUE",
            help="Environment variable assignment for the process/container (repeatable).",
        ),
        showConfig: bool = typer.Option(
            False,
            "--show-config",
            help="Print effective kub-cli configuration as JSON.",
        ),
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

        executeWrapperCommand(
            appName=appName,
            ctx=ctx,
            runtime=runtime,
            image=image,
            bind=bind or [],
            pwd=pwd,
            runner=runner,
            dryRun=dryRun,
            verbose=verbose,
            apptainerFlags=apptainerFlags or [],
            dockerFlags=dockerFlags or [],
            envVars=envVars or [],
            showConfig=showConfig,
        )

    return app


def createMetaApp() -> typer.Typer:
    app = typer.Typer(
        add_completion=False,
        no_args_is_help=False,
        help="kub-cli meta command.",
    )

    @app.callback(invoke_without_command=True)
    def meta(
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

        typer.echo(
            "kub-cli thin wrapper. Use: kub-dataset, kub-simulate, kub-dashboard, kub-img."
        )

    return app


datasetApp = createWrapperApp(
    appName="kub-dataset",
    helpText=(
        "Run the kub-dataset app inside the configured container runtime "
        "(Apptainer/Docker)."
    ),
)
simulateApp = createWrapperApp(
    appName="kub-simulate",
    helpText=(
        "Run the kub-simulate app inside the configured container runtime "
        "(Apptainer/Docker)."
    ),
)
dashboardApp = createWrapperApp(
    appName="kub-dashboard",
    helpText=(
        "Run the kub-dashboard app inside the configured container runtime "
        "(Apptainer/Docker)."
    ),
)
metaApp = createMetaApp()


def datasetMain() -> None:
    datasetApp(prog_name="kub-dataset")


def simulateMain() -> None:
    simulateApp(prog_name="kub-simulate")


def dashboardMain() -> None:
    dashboardApp(prog_name="kub-dashboard")


def metaMain() -> None:
    metaApp(prog_name="kub-cli")
