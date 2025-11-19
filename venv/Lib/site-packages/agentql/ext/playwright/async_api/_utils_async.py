import json
import logging
import time
from typing import Optional, Union

from playwright.async_api import Error, Frame, Page

from agentql._core._errors import AccessibilityTreeError, PageCrashError, PageMonitorNotInitializedError
from agentql._core._js_snippets.snippet_loader import load_js
from agentql.ext.playwright._driver_constants import (
    CURRENT_TF_ID_ATTR,
    SLOWEST_WEBSITE_AVG_LOAD_TIME_SECONDS,
    WEBSITE_AVG_LOAD_TIME_SECONDS,
)
from agentql.ext.playwright._network_monitor import PageActivityMonitor
from agentql.ext.playwright._utils import _post_process_accessibility_tree, merge_iframe_tree_into_page

"""
Utility functions for playwright async API.  Functions here are mirrored with _utils_sync.
"""

log = logging.getLogger("agentql")


async def add_request_event_listeners_for_page_monitor_shared(page: Page, monitor: Union[PageActivityMonitor, None]):
    if not monitor:
        raise PageMonitorNotInitializedError()

    try:
        page.on("request", monitor.track_network_request)
        page.on("requestfinished", monitor.track_network_response)
        page.on("requestfailed", monitor.track_network_response)
        page.on("load", monitor.track_load)
    # If the page is navigating, the evaluate function will raise an error. In this case, we wait for the page to load.
    except Error:
        start_time = time.time()
        while True:
            if monitor.get_load_status() or time.time() - start_time > WEBSITE_AVG_LOAD_TIME_SECONDS:
                break
            await page.wait_for_timeout(200)


async def add_dom_change_listener_shared(page: Page):
    try:
        await page.evaluate(load_js("add_dom_change_listener"))
    except Error:
        start_time = time.time()
        while True:
            if time.time() - start_time > WEBSITE_AVG_LOAD_TIME_SECONDS:
                break
            await page.wait_for_timeout(200)


async def determine_load_state_shared(
    page: Page,
    monitor: Union[PageActivityMonitor, None],
    timeout_seconds: float = SLOWEST_WEBSITE_AVG_LOAD_TIME_SECONDS,
    wait_for_network_idle: bool = True,
):
    if not monitor:
        raise PageMonitorNotInitializedError()

    if not wait_for_network_idle:
        await page.wait_for_load_state("load")
        log.debug("Page ready: 'load' event catched.")
        return

    start_time = time.time()

    while True:
        try:
            last_updated_timestamp = await page.evaluate(load_js("get_last_dom_change"))
        # If the page is navigating, the evaluate function will raise an error. In this case, we wait for the page to load.
        except Error:
            while True:
                if monitor.get_load_status() or time.time() - start_time > WEBSITE_AVG_LOAD_TIME_SECONDS:
                    break
                await page.wait_for_timeout(200)
            # monitor.check_conditions() is expecting milliseconds
            last_updated_timestamp = time.time() * 1000

        if monitor.is_page_ready(start_time=start_time, last_active_dom_time_ms=last_updated_timestamp):
            break

        if time.time() - start_time > timeout_seconds:
            log.debug("Page ready: Timeout while waiting for page to settle.")
            break
        await page.wait_for_timeout(100)


async def _process_iframes(
    page: Page,
    page_accessibility_tree: dict,
    *,
    iframe_path: str = "",
    frame: Optional[Frame] = None,
    include_hidden: bool,
):
    """
    Recursively retrieves the accessibility trees for all iframes in a page or frame.

    Parameters:
    ----------
    iframe_path (str): The path of the iframe in the frame hierarchy.
    frame (Frame): The frame object representing the current frame.
    page_accessibility_tree (dict): The accessibility tree of the page.
    """

    if not frame:
        main_frame = page.main_frame
        iframes = main_frame.child_frames
    else:
        iframes = frame.child_frames

    for iframe in iframes:
        if iframe.is_detached() or not await _iframe_contains_doc_or_body(iframe):
            continue
        iframe_handle = await iframe.frame_element()
        iframe_id = await iframe_handle.get_attribute("tf623_id")
        iframe_path_to_send = ""
        if iframe_path:
            iframe_path_to_send = f"{iframe_path}."
        iframe_path_to_send = f"{iframe_path_to_send}{iframe_id}"
        iframe_accessibility_tree = await _get_frame_accessibility_tree(iframe, iframe_path_to_send, include_hidden)

        merge_iframe_tree_into_page(iframe_id, page_accessibility_tree, iframe_accessibility_tree)

        await _process_iframes(
            page=page,
            iframe_path=iframe_path_to_send,
            frame=iframe,
            page_accessibility_tree=page_accessibility_tree,
            include_hidden=include_hidden,
        )


async def _iframe_contains_doc_or_body(frame: Frame) -> bool:
    """
    Checks if an iframe contains document or body.

    Parameters:
    ----------
    frame (Frame): The iframe to check.

    Returns:
    --------
    bool: True if iframes contains these elements, False otherwise.
    """
    return await frame.evaluate(load_js("iframe_contains_doc_or_body"))


async def _get_frame_accessibility_tree(frame: Frame, iframe_path, include_hidden) -> dict:
    """
    Retrieves the accessibility tree for a given frame.

    Parameters:
    ----------
    frame (Frame): The frame for which to retrieve the accessibility tree.
    iframe_path: The path of the iframe within the frame.

    Returns:
    --------
    dict: The accessibility tree for the frame.
    """
    await _set_iframe_path(context=frame, iframe_path=iframe_path)
    accessibility_tree = await _get_page_accessibility_tree(frame, include_hidden)

    return accessibility_tree


async def _get_page_accessibility_tree(context: Union[Page, Frame], include_hidden: bool) -> dict:
    """
    Retrieves the accessibility tree for the given page.

    Parameters:
    ----------
    context (Page | Frame): The context in which to retrieve the accessibility tree.
    include_hidden (bool, optional): Whether to include hidden elements in the tree.

    Returns:
    --------
    dict: The accessibility tree for the page.
    """
    page = context.page if isinstance(context, Frame) else context

    # Get the underlying impl object of the Page object so that we could set the current tf id attribute directly on it.
    # pylint: disable=protected-access
    page = page._impl_obj

    current_tf_id = getattr(page, CURRENT_TF_ID_ATTR, 0)
    # Need to concatenate the function name in the evaluate expression parameter
    # because of the comment before the function.
    result_json = await context.evaluate(
        load_js("generate_accessibility_tree")
        + "\n"
        + """
        (arg) => {
            const res = generateAccessibilityTree(arg);
            return JSON.stringify(res);
        }
        """,
        {
            "currentGlobalId": current_tf_id,
            "processIFrames": False,
            "includeHidden": include_hidden,
        },
    )
    result: dict = json.loads(result_json)

    setattr(page, CURRENT_TF_ID_ATTR, result.get("lastUsedId"))
    tree = result.get("tree")
    if not tree:
        raise ValueError("There is no tree in the gen tree result")

    return tree


async def _set_iframe_path(context: Union[Page, Frame], iframe_path=None):
    """
    Sets the iframe path in the given context.

    Parameters:
    ----------
    context (Page | Frame): The context in which the DOM will be modified.
    iframe_path (str, optional): The path to the iframe. Defaults to None.
    """
    await context.evaluate(
        load_js("set_iframe_path"),
        {"iframe_path": iframe_path},
    )


async def handle_page_crash(page: Page):
    log.error(f"Page crashed: {page.url}")
    await page.close()
    raise PageCrashError(page.url)


async def get_accessibility_tree(page: Page, include_hidden: bool) -> dict:
    """
    Gets the accessibility tree for a page.

    Parameters:
    ----------
    page (Page): The page for which to get the accessibility tree.
    include_hidden (bool): Whether to include hidden elements in the tree.
    """
    try:
        page_accessibility_tree = await _get_page_accessibility_tree(page, include_hidden)
        await _process_iframes(page, page_accessibility_tree, include_hidden=include_hidden)
        _post_process_accessibility_tree(page_accessibility_tree)
        return page_accessibility_tree
    except Exception as e:
        raise AccessibilityTreeError() from e
