import logging
from typing import TYPE_CHECKING, Union

from playwright.sync_api import Locator as _Locator
from playwright.sync_api import Page as PlaywrightPage
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from agentql import ContainerListNode, ContainerNode, IdListNode, IdNode
from agentql.ext.playwright._response_proxy_base import BaseAQLResponseProxy
from agentql.ext.playwright._utils import locate_interactive_element
from agentql.ext.playwright.constants import (
    DEFAULT_PAGINATION_CLICK_FORCE,
    DEFAULT_PAGINATION_CLICK_TIMEOUT_MS,
)

log = logging.getLogger("agentql")


class AQLResponseProxy(BaseAQLResponseProxy["Locator"]):
    """
    AQLResponseProxy class acts as a dynamic proxy to the response received from AgentQL server. It allows users to interact with resulting web elements and to retrieve their text contents. This class is designed to work with the web driver to fetch and process query results.
    """

    if TYPE_CHECKING:
        # needed to make the type checker happy
        def __call__(self, *args, **kwargs): ...

    def __init__(
        self,
        data: Union[dict, list],
        page: PlaywrightPage,
        query_tree: ContainerNode,
    ):
        self._response_data = data
        self._page = page
        self._query_tree_node = query_tree

    def __getattr__(self, name) -> Union["Locator", "AQLResponseProxy"]:
        return super().__getattr__(name)

    def __getitem__(self, index: int) -> Union["Locator", "AQLResponseProxy"]:
        return super().__getitem__(index)

    def _resolve_item(self, item, query_tree_node) -> Union["Locator", "AQLResponseProxy", None]:
        if item is None:
            return None

        if isinstance(item, list):
            return AQLResponseProxy(item, self._page, query_tree_node)

        if isinstance(query_tree_node, (IdNode, IdListNode)):
            interactive_element: Locator = locate_interactive_element(self._page, item)  # type: ignore
            log.debug(f"Resolved to {interactive_element}")

            return interactive_element

        return AQLResponseProxy(item, self._page, query_tree_node)

    def to_data(self) -> dict:
        """
        Converts the response data into a structured dictionary based on the query tree.

        Returns:
        --------
        dict: A structured dictionary representing the processed response data, with fact nodes replaced by name (values) from the response data. It will have the following structure:

        ```py
        {
            "query_field": "text content of the corresponding web element"
        }
        ```
        """
        return self._to_data_node(self._response_data, self._query_tree_node)

    def _to_data_node(self, response_data, query_tree_node) -> dict:
        if isinstance(query_tree_node, ContainerListNode):
            return self._to_data_container_list_node(response_data, query_tree_node)  # type: ignore
        elif isinstance(query_tree_node, ContainerNode):
            return self._to_data_container_node(response_data, query_tree_node)
        elif isinstance(query_tree_node, IdListNode):
            return self._to_data_id_list_node(response_data)  # type: ignore
        elif isinstance(query_tree_node, IdNode):
            return self._to_data_id_node(response_data)  # type: ignore
        else:
            raise TypeError("Unsupported query tree node type")

    def _to_data_container_node(self, response_data: dict, query_tree_node: ContainerNode) -> dict:
        results = {}
        for child_name, child_data in response_data.items():
            child_query_tree = query_tree_node.get_child_by_name(child_name)
            results[child_name] = self._to_data_node(child_data, child_query_tree)
        return results

    def _to_data_container_list_node(self, response_data: dict, query_tree_node: ContainerListNode) -> list:
        return [self._to_data_container_node(item, query_tree_node) for item in response_data]

    def _to_data_id_node(self, response_data: dict) -> Union[dict, str, None]:
        if response_data is None:
            return None
        name = response_data.get("name")
        if not name or not name.strip():
            web_element: Locator = locate_interactive_element(self._page, response_data)  # type: ignore
            if not web_element:
                log.warning(f"Could not locate web element for item {response_data}")
                return None
            element_text = web_element.text_content()
            if not element_text:
                log.warning(f"Could not get text content for item {response_data}")
                return None
            name = element_text.strip()
        return name

    def _to_data_id_list_node(self, response_data: list) -> list:
        return [node for node in (self._to_data_id_node(item) for item in response_data) if node is not None]


class Locator(_Locator):
    if TYPE_CHECKING:

        def __call__(self, *args, **kwargs): ...
        def __getattr__(self, name) -> Union[AQLResponseProxy, "Locator"]: ...
        def __getitem__(self, index: int) -> Union[AQLResponseProxy, "Locator"]: ...
        def __len__(self) -> int: ...


class PaginationInfo:
    """
    PaginationInfo class stores data related to pagination. For example, web elements that that trigger navigation to the next page.
    """

    def __init__(
        self,
        next_page_element: Union[Locator, None],
    ):
        self._next_page_element = next_page_element

    @property
    def has_next_page(self) -> bool:
        return self._next_page_element is not None

    def navigate_to_next_page(self, force_click: bool = DEFAULT_PAGINATION_CLICK_FORCE):
        if not self._next_page_element:
            log.error("Attempting to navigate to the next page while no pagination element is present.")
        else:
            try:
                log.info("Clicking on the pagination element...")
                self._next_page_element.click(force=force_click, timeout=DEFAULT_PAGINATION_CLICK_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                log.exception(
                    f"Timeout of {DEFAULT_PAGINATION_CLICK_TIMEOUT_MS}ms exceeded while clicking on the pagination element."
                )
            except Exception as e:  # pylint: disable=broad-except
                log.exception(f"Error while clicking on the pagination element: {e}")
