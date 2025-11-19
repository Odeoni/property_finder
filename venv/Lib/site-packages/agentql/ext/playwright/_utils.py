import json
from typing import Optional, Union

from playwright.async_api import FrameLocator as AsyncFrameLocator
from playwright.async_api import Locator as AsyncLocator
from playwright.async_api import Page as AsyncPage
from playwright.sync_api import FrameLocator as SyncFrameLocator
from playwright.sync_api import Locator as SyncLocator
from playwright.sync_api import Page as SyncPage

from agentql._core._errors import ElementNotFoundError


def _post_process_accessibility_tree(accessibility_tree: dict):
    """Post-process the accessibility tree by removing node's attributes that are Null."""
    if "children" in accessibility_tree and accessibility_tree.get("children") is None:
        del accessibility_tree["children"]

    if isinstance(accessibility_tree.get("children"), str):
        try:
            accessibility_tree["children"] = json.loads(accessibility_tree["children"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Accessibility tree contains children of type str and is not valid JSON: {e}") from e

    for child in accessibility_tree.get("children", []):
        _post_process_accessibility_tree(child)


def merge_iframe_tree_into_page(iframe_id, accessibility_tree: dict, iframe_accessibility_tree: dict):
    """
    Stitches the iframe accessibility tree with the page accessibility tree.

    Parameters:
    ----------
    iframe_id (str): The ID of the iframe.
    accessibility_tree (dict): The accessibility tree of the page.
    iframe_accessibility_tree (dict): The accessibility tree of the iframe.

    Returns:
    --------
    None
    """
    children = accessibility_tree.get("children", []) or []
    for child in children:
        attributes = child.get("attributes", {})
        if "tf623_id" in attributes and attributes["tf623_id"] == iframe_id:
            if not child.get("children"):
                child["children"] = []
            child["children"].append(iframe_accessibility_tree)
            break
        merge_iframe_tree_into_page(iframe_id, child, iframe_accessibility_tree)


def locate_interactive_element(
    page: Union[AsyncPage, SyncPage], element_data: dict
) -> Union[AsyncLocator, SyncLocator]:
    """
    Locates an interactive element in the web page.

    Parameters:
    -----------
    page (Page): The page to search for the element in.
    element_data (dict): The data of the interactive element from the AgentQL response.

    Returns:
    --------
    Locator: The located interactive element. Return type will be Async if input `Page` is Async.

    Raises:
    ------
    ElementNotFoundError: If TF ID is missing from `element_data` or the element with the specified TF ID is not found.
    """

    tf623_id = element_data.get("tf623_id")
    if not tf623_id:
        raise ElementNotFoundError(page.url, "tf623_id")
    iframe_path = element_data.get("attributes", {}).get("iframe_path")
    interactive_element = find_element_by_id(page=page, tf623_id=tf623_id, iframe_path=iframe_path)
    return interactive_element


def find_element_by_id(
    page: Union[AsyncPage, SyncPage], tf623_id: str, iframe_path: str = ""
) -> Union[AsyncLocator, SyncLocator]:
    """
    Finds an element by its TF ID within a specified iframe.

    Parameters:
    ----------
    page (Page): The page to search for the element in.
    tf623_id (str): The generated tf id of the element to find.
    iframe_path (str): The path to the iframe containing the element.

    Returns:
    --------
    Locator: The located element. Return type will be Async if input `Page` is Async.

    Raises:
    ------
    ElementNotFoundError: If the element with the specified TF ID is not found.
    """
    try:
        element_frame_context = _get_frame_context(page=page, iframe_path=iframe_path)
        return element_frame_context.locator(f"[tf623_id='{tf623_id}']")  # type: ignore
    except Exception as e:
        raise ElementNotFoundError(page.url, tf623_id) from e


def _get_frame_context(
    page: Union[AsyncPage, SyncPage], iframe_path: Optional[str] = None
) -> Union[AsyncFrameLocator, SyncFrameLocator, AsyncPage, SyncPage]:
    """
    Returns the frame context for the given iframe path.

    Parameters:
    ----------
    page (Page): The page to retrieve the end frame context from.
    iframe_path (str): The path of the iframe within the frame.

    Returns:
    --------
    Frame | Page: The frame context for the given iframe path. Return type will be Async if input `Page` is Async.
    """
    if not iframe_path:
        return page

    iframe_path_list = iframe_path.split(".")
    frame_context = page
    for iframe_id in iframe_path_list:
        frame_context = frame_context.frame_locator(f"[tf623_id='{iframe_id}']")
    return frame_context
