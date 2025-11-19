import logging
from typing import Union

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from agentql import ResponseMode
from agentql._core._api_constants import DEFAULT_RESPONSE_MODE
from agentql.ext.playwright.async_api.playwright_smart_locator import Page
from agentql.ext.playwright.async_api.response_proxy import Locator
from agentql.ext.playwright.constants import (
    DEFAULT_INCLUDE_HIDDEN_DATA,
    DEFAULT_PAGINATION_CLICK_FORCE,
    DEFAULT_PAGINATION_CLICK_TIMEOUT_MS,
    DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
    DEFAULT_WAIT_FOR_NETWORK_IDLE,
)
from agentql.ext.playwright.tools._shared.pagination._prompts import (
    generate_next_page_element_prompt,
)

log = logging.getLogger("agentql")


async def paginate(
    page: Page,
    query: str,
    number_of_pages: int,
    timeout: int = DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
    wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
    include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_DATA,
    mode: ResponseMode = DEFAULT_RESPONSE_MODE,
    force_click: bool = DEFAULT_PAGINATION_CLICK_FORCE,
) -> list[dict]:
    """
    Paginate over specified number of pages and aggregate all returned data.

    This method will:
    1. Call `_get_current_page_info` to extract data on the current page and locate a pagination element for navigating to the next page.
    2. Click on the pagination element to navigate to the next page.
    3. Repeat the process until all pages are visited.
    4. Can halt the pagination and return aggregated data if at any iteration pagination element is not found or fails to be clicked.

    Parameters:
    -----------
    page (Page): An AgentQL Page object.
    query (str): An AgentQL query in String format.
    number_of_pages (int): Number of pages to paginate over.
    timeout (int) (optional): Timeout value in seconds for extracting data from one page.
    wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying each page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
    include_hidden (bool) (optional): Whether to include hidden elements on each page. Defaults to `True`.
    mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.
    force_click (bool) (optional): Whether to force click on the pagination element. Defaults to `False`.

    Returns:
    --------
    List[dict]: List of dictionaries containing the data from each page.
    """

    log.debug("Starting Pagination")
    data = []
    for p in range(number_of_pages):
        log.debug(f"Querying Page {p + 1}")
        is_last_page = p == number_of_pages - 1
        extracted_data, pagination_element = await _get_current_page_info(
            page,
            query,
            is_last_page,
            timeout,
            wait_for_network_idle,
            include_hidden,
            mode,
        )
        data.append(extracted_data)

        # if it's the last page, do not navigate to the next page
        if is_last_page:
            break

        # if pagination element is None, end pagination process
        if pagination_element is None:
            log.debug("No pagination element found. Reaching the end of pagination process.")
            break

        # click on the next page element
        try:
            await pagination_element.click(force=force_click, timeout=DEFAULT_PAGINATION_CLICK_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            log.debug(
                f"Timeout error while clicking on the pagination element (Timeout of {DEFAULT_PAGINATION_CLICK_TIMEOUT_MS}ms exceeded). Halting the pagination process. The data gathered so far will be returned."
            )
            break
        except Exception as e:  # pylint: disable=broad-except
            log.debug(
                f"An error occurred while attempting to click on the pagination element. Halting the pagination process. The data gathered so far will be returned. Error: {e}."
            )
            break

    return data


async def _get_current_page_info(
    page: Page,
    query: str,
    is_last_page: bool,
    timeout: int,
    wait_for_network_idle: bool,
    include_hidden: bool,
    mode: ResponseMode,
) -> tuple[dict, Union[Locator, None]]:
    """
    Extract data from current page and find a locator for next page navigation.

    Returns:
    --------
    Tuple[dict, Union[Locator, None]]: Tuple containing the data and the pagination element.
    """

    response = await page.query_data(
        query=query,
        timeout=timeout,
        wait_for_network_idle=wait_for_network_idle,
        include_hidden=include_hidden,
        mode=mode,
    )

    if is_last_page:
        pagination_element = None
    else:
        pagination_element = await page.get_by_prompt(prompt=generate_next_page_element_prompt(query))

    return response, pagination_element
