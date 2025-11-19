import subprocess
from configparser import ConfigParser

import typer

from agentql._core._utils import CONFIG_FILE_PATH, DEBUG_FILE_PATH
from agentql.sync_api._agentql_service import check_agentql_server_status, validate_api_key

from ._utils import download_script

SAMPLE_SCRIPT_URL = (
    "https://raw.githubusercontent.com/tinyfish-io/agentql/main/examples/python/first_steps/first_steps.py"
)
DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 10
SAMPLE_SCRIPT_FILE_NAME = "example_script.py"


def _install_dependencies():
    typer.echo("Installing dependencies...")
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode().strip()
        typer.echo(
            f"Failed to install dependencies: {error_msg}. If the issue persists, please email support@tinyfish.io",
            err=True,
        )
        raise typer.Exit(code=1)


def _request_api_key():
    typer.echo("Get your AgentQL API key at https://dev.agentql.com")
    api_key = typer.prompt("Enter your AgentQL API key")
    if not api_key:
        typer.echo("API key cannot be empty.", err=True)
        raise typer.Exit(code=1)
    return api_key


def _check_server_status():
    typer.echo("Checking AgentQL server status...")
    if not check_agentql_server_status():
        typer.echo(
            "AgentQL server is currently unavailable. Please check your internet connection or try again later. If the issue persists, please email support@tinyfish.io",
            err=True,
        )
        raise typer.Exit(code=1)


def _check_api_key(api_key: str):
    typer.echo("Validating AgentQL API key...")
    if not validate_api_key(api_key):
        typer.echo(
            "Invalid AgentQL API key. Please double check the API Key. If the issue persists, please email support@tinyfish.io",
            err=True,
        )
        raise typer.Exit(code=1)


def _save_config_to_file(api_key: str, debug_path: str):
    config = ConfigParser()
    config["DEFAULT"] = {"agentql_api_key": api_key, "agentql_debug_path": debug_path}

    try:
        CONFIG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as configfile:
            config.write(configfile)
    except Exception as e:
        typer.echo(
            f"Failed to write configuration file: {str(e)}. If the issue persists, please email support@tinyfish.io",
            err=True,
        )
        raise typer.Exit(code=1)


def _download_sample_script():
    want_to_download = typer.confirm(
        "Would you like to download an example script to get started?",
        default=True,
    )
    if not want_to_download:
        return

    typer.echo("Downloading the example script...")
    download_script(SAMPLE_SCRIPT_URL, SAMPLE_SCRIPT_FILE_NAME)


def _request_debug_files_path():
    debug_path = typer.prompt(
        "Debug files path (for storing troubleshooting logs)",
        default=str(DEBUG_FILE_PATH),
    )
    return debug_path


def init():
    """Initialize the agentql project."""
    _install_dependencies()
    api_key = _request_api_key()
    _check_server_status()
    _check_api_key(api_key)
    debug_path = _request_debug_files_path()
    _save_config_to_file(api_key=api_key, debug_path=debug_path)
    typer.echo(f"AgentQL Configuration file saved successfully to {CONFIG_FILE_PATH}.")
    _download_sample_script()
    typer.echo(
        "AgentQL is now installed and ready to use! Get started with your first script by running `python3 example_script.py`. For more examples and detailed documentation, visit our website at https://docs.agentql.com/getting-started/first-steps."
    )
