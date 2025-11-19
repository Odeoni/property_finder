import asyncio
import functools
import json
import logging
import os
import warnings
from configparser import ConfigParser
from pathlib import Path
from typing import Optional, Union

import aiofiles
import httpx
import requests
from colorama import Fore, Style

from agentql import APIKeyError
from agentql._core._config import config
from agentql._core._errors import INVALID_API_KEY_MESSAGE

logger = logging.getLogger(__name__)

API_KEY_FILE_PATH_BEFORE_0_5_0 = Path.home() / ".agentql" / "config" / "agentql_api_key.ini"
CONFIG_FILE_PATH = Path.home() / ".agentql" / "config" / "config.ini"
DEBUG_FILE_PATH = Path.home() / ".agentql" / "debug"


def ensure_url_scheme(url: str) -> str:
    """
    Ensure that the URL has a scheme.
    """
    if not url.startswith(("http://", "https://", "file://")):
        return "https://" + url
    return url


def minify_query(query: str) -> str:
    """
    Minify the query by removing all newlines and extra spaces.
    """
    return query.replace("\n", "\\").replace(" ", "")


def get_api_key() -> Optional[str]:
    """
    Get the AgentQL API key from a configuration file or an environment variable.

    Returns:
    -------
    Optional[str]: The API key if found, None otherwise.
    """
    api_key = config.get_api_key()
    if not api_key:
        # Fallback to the config file if the key wasn't found in the environment variable
        local_config = ConfigParser()

        # Migrate the API key from the old config file to the new one
        if os.path.exists(API_KEY_FILE_PATH_BEFORE_0_5_0):
            local_config.read(API_KEY_FILE_PATH_BEFORE_0_5_0)
            api_key = local_config.get("DEFAULT", "agentql_api_key", fallback=None)
            if api_key:
                with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as file:
                    local_config.write(file)
            os.remove(API_KEY_FILE_PATH_BEFORE_0_5_0)

        if os.path.exists(CONFIG_FILE_PATH):
            logger.debug("Using API key from local file...")
            local_config.read(CONFIG_FILE_PATH)
            api_key = local_config.get("DEFAULT", "agentql_api_key", fallback=None)
            if api_key:
                return api_key

    return api_key


async def get_api_key_async() -> Optional[str]:
    """
    Get the AgentQL API key from a configuration file or an environment variable asynchronously.

    Returns:
    -------
    Optional[str]: The API key if found, None otherwise.
    """
    api_key = config.get_api_key()
    if not api_key:
        # Fallback to the config file if the key wasn't found in the environment variable
        local_config = ConfigParser()

        # Migrate the API key from the old config file to the new one
        if os.path.exists(API_KEY_FILE_PATH_BEFORE_0_5_0):
            async with aiofiles.open(API_KEY_FILE_PATH_BEFORE_0_5_0) as file:
                content = await file.read()
            local_config.read_string(content)
            api_key = local_config.get("DEFAULT", "agentql_api_key", fallback=None)
            if api_key:
                async with aiofiles.open(CONFIG_FILE_PATH, mode="w") as file:
                    await file.write(content)
            os.remove(API_KEY_FILE_PATH_BEFORE_0_5_0)

        if os.path.exists(CONFIG_FILE_PATH):
            logger.debug("Using API key from local file...")
            async with aiofiles.open(CONFIG_FILE_PATH) as file:
                content = await file.read()
            local_config.read_string(content)

        api_key = local_config.get("DEFAULT", "agentql_api_key", fallback=None)
        if api_key:
            return api_key

    return api_key


def get_debug_files_path() -> str:
    """
    Get the path to the debug files directory through environment variables or a configuration file.

    Returns:
    -------
    str: The path to the debug files directory.
    """

    env_debug_path = os.getenv("AGENTQL_DEBUG_PATH")
    if env_debug_path is not None:
        return env_debug_path

    debug_path = ""
    try:
        config = ConfigParser()
        config.read(CONFIG_FILE_PATH)
        debug_path = config.get("DEFAULT", "agentql_debug_path", fallback=None)
    except FileNotFoundError:
        pass

    return debug_path or str(DEBUG_FILE_PATH)


def save_json_file(path, data):
    """Save a JSON file.

    Parameters:
    ----------
    path (str): The path to the JSON file.
    data (dict): The data to save.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def save_text_file(path, text):
    """Save a text file.

    Parameters:
    ----------
    path (str): The path to the text file.
    text (str): The text to save.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def experimental_api(func):
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        warnings.warn(
            f"{Fore.RED}🚨 The function {func.__name__} is experimental and may not work as expected 🚨{Style.RESET_ALL}",
            category=UserWarning,
        )
        return func(*args, **kwargs)

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        warnings.warn(
            f"{Fore.RED}🚨 The function {func.__name__} is experimental and may not work as expected 🚨{Style.RESET_ALL}",
            category=UserWarning,
        )
        return await func(*args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


def raise_401_error(
    server_error: Union[requests.exceptions.HTTPError, httpx.HTTPStatusError],
    request_id: Optional[str],
) -> None:
    server_error_message = _get_server_error_message(server_error)
    raise APIKeyError(
        message=server_error_message or INVALID_API_KEY_MESSAGE,
        request_id=request_id,
    ) from server_error


def _get_server_error_message(
    server_error: Union[requests.exceptions.HTTPError, httpx.HTTPStatusError],
) -> Optional[str]:
    try:
        error_dict = (
            server_error.response.json()
            if isinstance(server_error, httpx.HTTPStatusError)
            else json.loads(server_error.response.text)
        )
        return error_dict["detail"] if error_dict else None
    except (KeyError, ValueError):
        return None
