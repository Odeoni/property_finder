import requests
import typer

DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 10


def download_script(url: str, file_name: str):
    """Download a script from the given URL."""
    try:
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT_IN_SECONDS)
        response.raise_for_status()

        with open(file_name, "wb") as file:
            file.write(response.content)
    except requests.RequestException:
        typer.echo(
            f"Error downloading the example script. Example script could be found at {url}.",
            err=True,
        )
