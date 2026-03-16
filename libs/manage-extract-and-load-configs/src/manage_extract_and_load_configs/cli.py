from __future__ import annotations

from pathlib import Path

import typer

from manage_extract_and_load_configs.uploader import delete_configs_from_blob, upload_configs_to_blob

app = typer.Typer(help="Manage extract and load config files.", no_args_is_help=True)


@app.callback()
def callback() -> None:
    """CLI entrypoint."""


@app.command("upload")
def upload(
    source_path: Path = typer.Option(
        ...,
        "--source-path",
        "-s",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a YAML file or directory containing YAML files.",
    ),
    variables_path: Path | None = typer.Option(
        None,
        "--variables-path",
        "-v",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Optional path to YAML variables used for Jinja rendering. If omitted, uses MANAGE_EL_ENV from .env.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be uploaded without sending files.",
    ),
) -> None:
    """Upload extract and load config files to blob storage."""
    try:
        result = upload_configs_to_blob(
            source_path=source_path,
            variables_path=variables_path,
            dry_run=dry_run,
        )
    except Exception as exc:  # pragma: no cover - CLI surface
        raise typer.BadParameter(str(exc)) from exc

    action = "Would upload" if dry_run else "Uploaded"
    typer.echo(f"{action} {result.uploaded_count} files to container '{result.destination_container}'.")
    typer.echo(f"Total bytes: {result.total_bytes}")


@app.command("destroy")
def destroy(
    source_path: Path = typer.Option(
        ...,
        "--source-path",
        "-s",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a YAML file or directory containing YAML files.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be deleted without sending requests.",
    ),
    ignore_missing: bool = typer.Option(
        True,
        "--ignore-missing/--fail-on-missing",
        help="Ignore missing blobs by default.",
    ),
) -> None:
    """Delete extract and load config blobs from storage."""
    try:
        result = delete_configs_from_blob(
            source_path=source_path,
            dry_run=dry_run,
            ignore_missing=ignore_missing,
        )
    except Exception as exc:  # pragma: no cover - CLI surface
        raise typer.BadParameter(str(exc)) from exc

    action = "Would delete" if dry_run else "Deleted"
    typer.echo(f"{action} {result.deleted_count} blobs from container '{result.destination_container}'.")
    typer.echo(f"Matched YAML files: {result.total_candidates}")


if __name__ == "__main__":
    app()
