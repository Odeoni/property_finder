import logging
from pathlib import Path
from typing import Optional

import aiofiles

from agentql import QueryParser, ResponseMode
from agentql._core._api_constants import DEFAULT_RESPONSE_MODE
from agentql.async_api._agentql_service import query_agentql_server_document
from agentql.ext.playwright.constants import DEFAULT_QUERY_DATA_TIMEOUT_SECONDS

log = logging.getLogger("agentql")


async def query_document(
    file_path: str,
    query: Optional[str] = None,
    prompt: Optional[str] = None,
    timeout: int = DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
    mode: ResponseMode = DEFAULT_RESPONSE_MODE,
    **kwargs,
) -> dict:
    """
    Query a document using AgentQL.

    Parameters:
    -----------
    file_path: str
        The file path of the file to query (string).
    query: Optional[str]
        The query to execute. Either query or prompt must be provided, but not both.
    prompt: Optional[str]
        The prompt to use for the query. Either query or prompt must be provided, but not both.
    timeout: int
        The timeout for the query in seconds.
    mode: ResponseMode
        The mode to use for the query.

    Returns:
    --------
    dict: The response from the query.
    """

    if query:
        try:
            QueryParser(query).parse()
        except Exception as e:
            raise ValueError("Invalid query.") from e

    if not query and not prompt:
        raise ValueError("Either query or prompt must be provided.")
    if query and prompt:
        raise ValueError("Only one of query or prompt must be provided.")
    file_object: dict[str, tuple[Optional[str], bytes]]
    file = Path(file_path)
    if not file.is_file():
        raise FileNotFoundError("File not found.")

    async with aiofiles.open(file, "rb") as f:
        file_object = {"file": (file.name, await f.read())}
    response = await query_agentql_server_document(
        file=file_object,
        query=query,
        prompt=prompt,
        timeout=timeout,
        mode=mode,
        **kwargs,
    )

    return response
