import asyncio
import logging
from typing import TYPE_CHECKING, Union

from playwright.async_api import Locator as _Locator
from playwright.async_api import Page as PlaywrightPage
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

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
        async def __call__(self, *args, **kwargs): ...

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

    async def to_data(self) -> dict:
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
        return await self._to_data_node(self._response_data, self._query_tree_node)

    async def _to_data_node(self, response_data, query_tree_node) -> dict:
        if isinstance(query_tree_node, ContainerListNode):
            return await self._to_data_container_list_node(response_data, query_tree_node)  # type: ignore
        elif isinstance(query_tree_node, ContainerNode):
            return await self._to_data_container_node(response_data, query_tree_node)
        elif isinstance(query_tree_node, IdListNode):
            return await self._to_data_id_list_node(response_data)  # type: ignore
        elif isinstance(query_tree_node, IdNode):
            return await self._to_data_id_node(response_data)  # type: ignore
        else:
            raise TypeError("Unsupported query tree node type")

    async def _to_data_container_node(self, response_data: dict, query_tree_node: ContainerNode) -> dict:
        tasks = []
        children = []

        for child_name, child_data in response_data.items():
            child_query_tree = query_tree_node.get_child_by_name(child_name)

            task = asyncio.create_task(self._to_data_node(child_data, child_query_tree))
            tasks.append(task)
            children.append(child_name)

        # run all tasks concurrently.
        results = await asyncio.gather(*tasks)

        return {child_name: result for result, child_name in zip(results, children)}

    async def _to_data_container_list_node(self, response_data: dict, query_tree_node: ContainerListNode) -> list:
        tasks = [self._to_data_container_node(item, query_tree_node) for item in response_data]
        result_list = await asyncio.gather(*tasks)
        return result_list

    async def _to_data_id_node(self, response_data: dict) -> Union[dict, str, None]:
        if response_data is None:
            return None
        name = response_data.get("name")
        if not name or not name.strip():
            web_element: Locator = locate_interactive_element(self._page, response_data)  # type: ignore
            if not web_element:
                log.warning(f"Could not locate web element for item {response_data}")
                return None
            element_text = await web_element.text_content()
            if not element_text:
                log.warning(f"Could not get text content for item {response_data}")
                return None
            name = element_text.strip()
        return name

    async def _to_data_id_list_node(self, response_data: list) -> list:
        tasks = [self._to_data_id_node(item) for item in response_data]
        res = await asyncio.gather(*tasks)
        return [node for node in res if node is not None]


class Locator(_Locator):
    if TYPE_CHECKING:

        async def __call__(self, *args, **kwargs): ...
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

    async def navigate_to_next_page(self, force_click: bool = DEFAULT_PAGINATION_CLICK_FORCE):
        if not self._next_page_element:
            log.error("Attempting to navigate to the next page while no pagination element is present.")
        else:
            try:
                log.info("Clicking on the pagination element...")
                await self._next_page_element.click(force=force_click, timeout=DEFAULT_PAGINATION_CLICK_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                log.exception(
                    f"Timeout of {DEFAULT_PAGINATION_CLICK_TIMEOUT_MS}ms exceeded while clicking on the pagination element."
                )
            except Exception as e:  # pylint: disable=broad-except
                log.exception(f"Error while clicking on the pagination element: {e}")
