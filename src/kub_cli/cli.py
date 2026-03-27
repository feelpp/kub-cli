# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Typer CLI entrypoints for kub-cli wrapper commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from click.shell_completion import BashComplete, FishComplete, ShellComplete, ZshComplete
import typer
from typer.main import get_command

from . import __version__
from .commands import WrapperOptions, runWrapperCommand
from .config import KubConfigOverrides, loadKubConfig
from .errors import KubCliError
from .preflight import (
    DEFAULT_DOCTOR_CACHE_TTL_SECONDS,
    formatDoctorReport,
    reportHasRequiredFailures,
    runSystemDoctor,
)
from .versioning import bumpProjectVersion


CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "allow_interspersed_args": False,
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
    cemdbRoot: str | None,
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
        cemdbRoot=cemdbRoot,
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
        cemdbRoot: str | None = typer.Option(
            None,
            "--cemdb-root",
            metavar="PATH",
            help=(
                "Host CEMDB root directory mounted as /cemdb inside container "
                "(defaults to current directory)."
            ),
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
            cemdbRoot=cemdbRoot,
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
            typer.echo(
                "kub-cli thin wrapper. Use: kub-dataset, kub-simulate, kub-dashboard, kub-img."
            )

    @app.command("bump")
    def bumpCommand(
        part: str = typer.Argument(
            "patch",
            metavar="PART",
            help="Semantic version part to bump: major, minor, patch.",
        ),
        toVersion: str | None = typer.Option(
            None,
            "--to",
            metavar="VERSION",
            help="Set an explicit semantic version (MAJOR.MINOR.PATCH).",
        ),
        projectRoot: Path = typer.Option(
            Path("."),
            "--project-root",
            metavar="PATH",
            help="Project root containing pyproject.toml.",
        ),
        dryRun: bool = typer.Option(
            False,
            "--dry-run",
            help="Print planned version changes without writing files.",
        ),
    ) -> None:
        try:
            result = bumpProjectVersion(
                projectRoot=projectRoot.resolve(),
                part=part,
                toVersion=toVersion,
                dryRun=dryRun,
            )
        except KubCliError as error:
            typer.secho(str(error), fg=typer.colors.RED, err=True)
            raise typer.Exit(code=error.exit_code) from error

        if result.changed:
            action = "Planned" if dryRun else "Updated"
            typer.echo(f"{action} version: {result.oldVersion} -> {result.newVersion}")
        else:
            typer.echo(f"Version unchanged: {result.newVersion}")

        typer.echo(f"pyproject: {result.pyprojectPath}")
        typer.echo(f"fallback: {result.initPath}")
        if result.changelogUpdated:
            typer.echo(f"changelog: {result.changelogPath}")
        elif result.changelogPath.exists():
            typer.echo(f"changelog: {result.changelogPath} (unchanged)")

        if result.changed:
            releaseTag = f"v{result.newVersion}"
            if dryRun:
                typer.echo("Release tag commands (after running bump without --dry-run):")
            else:
                typer.echo("Release tag commands:")
            typer.echo(f"git tag {releaseTag}")
            typer.echo(f"git push origin {releaseTag}")
            typer.echo(
                f'gh release create {releaseTag} --generate-notes --title "{releaseTag}"'
            )

    @app.command("generate-shell-completion")
    def generateShellCompletionCommand(
        shell: str = typer.Argument(
            ...,
            metavar="{bash,zsh,fish}",
            help="Target shell for completion script generation.",
        ),
    ) -> None:
        try:
            script = buildShellCompletionScript(shell)
        except KubCliError as error:
            typer.secho(str(error), fg=typer.colors.RED, err=True)
            raise typer.Exit(code=error.exit_code) from error

        typer.echo(script, nl=False)

    @app.command("doctor")
    def doctorCommand(
        runtime: str | None = typer.Option(
            None,
            "--runtime",
            metavar="{auto,apptainer,docker}",
            help="Runtime override for capability diagnostics.",
        ),
        image: str | None = typer.Option(
            None,
            "--image",
            metavar="IMAGE",
            help="Image override for capability diagnostics.",
        ),
        runner: str | None = typer.Option(
            None,
            "--runner",
            metavar="PATH",
            help="Runner override for capability diagnostics.",
        ),
        verbose: bool | None = typer.Option(
            None,
            "--verbose/--no-verbose",
            help="Enable or disable verbose logging.",
        ),
        jsonOutput: bool = typer.Option(
            False,
            "--json",
            help="Emit doctor report as JSON.",
        ),
        refresh: bool = typer.Option(
            False,
            "--refresh",
            help="Ignore cached diagnostics and probe the system again.",
        ),
        noCache: bool = typer.Option(
            False,
            "--no-cache",
            help="Do not read/write the doctor cache.",
        ),
        cacheTtlSeconds: int = typer.Option(
            DEFAULT_DOCTOR_CACHE_TTL_SECONDS,
            "--cache-ttl",
            min=0,
            help="Doctor cache TTL in seconds (default: 300).",
        ),
    ) -> None:
        try:
            config = loadKubConfig(
                overrides=KubConfigOverrides(
                    runtime=runtime,
                    image=image,
                    runner=runner,
                    verbose=verbose,
                )
            )
            report = runSystemDoctor(
                config=config,
                refresh=refresh,
                useCache=not noCache,
                cacheTtlSeconds=cacheTtlSeconds,
            )
        except KubCliError as error:
            typer.secho(str(error), fg=typer.colors.RED, err=True)
            raise typer.Exit(code=error.exit_code) from error

        if jsonOutput:
            typer.echo(json.dumps(report.toDict(), indent=2, sort_keys=True))
        else:
            typer.echo(formatDoctorReport(report))

        if reportHasRequiredFailures(report):
            raise typer.Exit(code=2)

        raise typer.Exit(code=0)

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


SHELL_COMPLETION_SHELLS: tuple[str, ...] = ("bash", "fish", "zsh")
SHELL_COMPLETION_CLASSES: dict[str, type[ShellComplete]] = {
    "bash": BashComplete,
    "fish": FishComplete,
    "zsh": ZshComplete,
}


def resolveShellCompletionShell(shell: str) -> str:
    normalized = shell.strip().lower()
    if normalized not in SHELL_COMPLETION_SHELLS:
        supported = ", ".join(sorted(SHELL_COMPLETION_SHELLS))
        raise KubCliError(
            f"Unsupported shell '{shell}'. Use one of: {supported}."
        )
    return normalized


def getCompletionApps() -> tuple[tuple[str, typer.Typer], ...]:
    from .img_cli import imgApp

    return (
        ("kub-cli", metaApp),
        ("kub-dataset", datasetApp),
        ("kub-simulate", simulateApp),
        ("kub-dashboard", dashboardApp),
        ("kub-img", imgApp),
    )


def buildShellCompletionScript(shell: str) -> str:
    normalizedShell = resolveShellCompletionShell(shell)
    completionClass = SHELL_COMPLETION_CLASSES[normalizedShell]
    scripts: list[str] = []
    for progName, app in getCompletionApps():
        clickCommand = get_command(app)
        completeVar = f"_{progName.replace('-', '_').upper()}_COMPLETE"
        completion = completionClass(
            cli=clickCommand,
            ctx_args={},
            prog_name=progName,
            complete_var=completeVar,
        ).source()
        completion = (
            completion
            .replace("=bash_complete", "=complete_bash")
            .replace("=zsh_complete", "=complete_zsh")
            .replace("=fish_complete", "=complete_fish")
        )
        scripts.append(completion.rstrip())

    return "\n\n".join(part for part in scripts if part) + "\n"


def datasetMain() -> None:
    datasetApp(prog_name="kub-dataset")


def simulateMain() -> None:
    simulateApp(prog_name="kub-simulate")


def dashboardMain() -> None:
    dashboardApp(prog_name="kub-dashboard")


def metaMain() -> None:
    metaApp(prog_name="kub-cli")
