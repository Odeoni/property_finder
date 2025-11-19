import enum

import typer

from ._utils import download_script

SYNC_TEMPLATE_URL = "https://raw.githubusercontent.com/tinyfish-io/agentql/main/.templates/python/template_sync.py"
ASYNC_TEMPLATE_URL = "https://raw.githubusercontent.com/tinyfish-io/agentql/main/.templates/python/template_async.py"
TEMPLATE_FILE_NAME = "template_script.py"


class ScriptType(enum.Enum):
    SYNC = "sync"
    ASYNC = "async"


def new_script(
    script_type: ScriptType = typer.Option(
        None,
        "-t",
        "--type",
        prompt="Choose the type of template script (sync or async)",
        help="Specify the type of template script between sync and async",
    ),
):
    """Download a new template script into the current directory."""
    typer.echo("Downloading the template script...")
    script_url = SYNC_TEMPLATE_URL if script_type == ScriptType.SYNC else ASYNC_TEMPLATE_URL
    download_script(script_url, TEMPLATE_FILE_NAME)
