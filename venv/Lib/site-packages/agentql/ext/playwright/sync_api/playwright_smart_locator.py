import logging
import time
from typing import Any, Literal, Optional, Union

from playwright.sync_api import Page as _Page
from playwright.sync_api import Response
from playwright_stealth import StealthConfig, stealth_sync
from playwright_stealth.core import BrowserType

from agentql import QueryParser
from agentql._core._api_constants import DEFAULT_RESPONSE_MODE
from agentql._core._errors import AgentQLServerTimeoutError
from agentql._core._syntax.node import ContainerNode
from agentql._core._typing import ResponseMode
from agentql._core._utils import experimental_api, minify_query
from agentql.ext.playwright._driver_constants import RENDERER, USER_AGENT, VENDOR
from agentql.ext.playwright._network_monitor import PageActivityMonitor
from agentql.ext.playwright._utils import find_element_by_id
from agentql.ext.playwright.constants import (
    DEFAULT_INCLUDE_HIDDEN_DATA,
    DEFAULT_INCLUDE_HIDDEN_ELEMENTS,
    DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
    DEFAULT_QUERY_ELEMENTS_TIMEOUT_SECONDS,
    DEFAULT_QUERY_GENERATE_TIMEOUT_SECONDS,
    DEFAULT_WAIT_FOR_NETWORK_IDLE,
)
from agentql.ext.playwright.sync_api.response_proxy import AQLResponseProxy, Locator, PaginationInfo
from agentql.ext.playwright.tools._shared.pagination._prompts import (
    generate_next_page_element_prompt,
)
from agentql.sync_api._agentql_service import (
    generate_query_from_agentql_server,
    query_agentql_server,
)

from ._utils_sync import (
    add_dom_change_listener_shared,
    add_request_event_listeners_for_page_monitor_shared,
    determine_load_state_shared,
    get_accessibility_tree,
    handle_page_crash,
)

log = logging.getLogger("agentql")


class Page(_Page):
    def __init__(self, page: _Page, page_monitor: PageActivityMonitor):  # pylint: disable=super-init-not-called
        # We intentionally not calling super().__init__ since this is a composition pattern
        # we inherit from Playwright Page class to maintain the same interface. But in reality all calls are forwarded to the underlying page object.
        self._page = page
        self._page_monitor = page_monitor
        self._last_query = None
        self._last_response = None
        self._last_accessibility_tree = None

    def __getattr__(self, name) -> Any:
        # Forward any attribute or method call to the underlying page object
        # This allows us to maintain the same interface without inheritance
        return getattr(self._page, name)

    @classmethod
    def create(cls, page: _Page):
        """
        Creates a new AgentQL Page instance with a page monitor initialized.

        Parameters:
        -----------
        page (Page): The Playwright page instance.

        Returns:
        --------
        Page: A new AgentQLPage instance with a page monitor initialized.
        """
        page_monitor = PageActivityMonitor()
        add_request_event_listeners_for_page_monitor_shared(page, page_monitor)
        add_dom_change_listener_shared(page)
        page.on("crash", handle_page_crash)

        return cls(page, page_monitor)

    def goto(
        self,
        url: str,
        *,
        timeout: Optional[float] = None,
        wait_until: Optional[Literal["commit", "domcontentloaded", "load", "networkidle"]] = "domcontentloaded",
        referer: Optional[str] = None,
    ) -> Optional[Response]:
        """
        AgentQL's `page.goto()` override that uses `domcontentloaded` as the default value for the `wait_until` parameter.
        This change addresses issue with the `load` event not being reliably fired on some websites.

        For parameters information and original method's documentation, please refer to
        [Playwright's documentation](https://playwright.dev/docs/api/class-page#page-goto)
        """
        result = self._page.goto(url=url, timeout=timeout, wait_until=wait_until, referer=referer)

        # Redirect will destroy the existing dom change listener, so we need to add it again.
        add_dom_change_listener_shared(self._page)
        return result

    def get_by_prompt(
        self,
        prompt: str,
        timeout: int = DEFAULT_QUERY_ELEMENTS_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_ELEMENTS,
        mode: ResponseMode = DEFAULT_RESPONSE_MODE,
        experimental_query_elements_enabled: bool = False,
        **kwargs,
    ) -> Union[Locator, None]:
        """
        Returns a single web element located by a natural language prompt (as opposed to a AgentQL query).

        Parameters:
        -----------
        prompt (str): The natural language description of the element to locate.
        timeout (int) (optional): Timeout value in seconds for the connection with backend API service.
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying the page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        include_hidden (bool) (optional): Whether to include hidden elements on the page. Defaults to `True`.
        mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.
        experimental_query_elements_enabled (bool) (optional): Whether to use the experimental implementation of the query elements feature. Defaults to `False`.

        Returns:
        --------
        Playwright [Locator](https://playwright.dev/python/docs/api/class-locator) | None: The found element or `None` if no matching elements were found.
        """
        query = f"""
        {{
            page_element({prompt})
        }}
        """

        response, _ = self._execute_query(
            query=query,
            timeout=timeout,
            include_hidden=include_hidden,
            wait_for_network_idle=wait_for_network_idle,
            mode=mode,
            is_data_query=False,
            experimental_query_elements_enabled=experimental_query_elements_enabled,
            **kwargs,
        )
        response_data = response.get("page_element")
        if not response_data:
            return None

        tf623_id = response_data.get("tf623_id")
        iframe_path = response_data.get("attributes", {}).get("iframe_path")
        web_element = find_element_by_id(page=self._page, tf623_id=tf623_id, iframe_path=iframe_path)

        return web_element  # type: ignore

    @experimental_api
    def get_data_by_prompt_experimental(
        self,
        prompt: str,
        timeout: int = DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_DATA,
        mode: ResponseMode = DEFAULT_RESPONSE_MODE,
        **kwargs,
    ) -> dict:  # type: ignore 'None' warning
        """
        Queries the web page for data that matches the natural language prompt (as opposed to a AgentQL query).

        Parameters:
        -----------
        prompt (str): The natural language description of the element to locate.
        timeout (int) (optional): Timeout value in seconds for the connection with backend API service.
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying the page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        include_hidden (bool) (optional): Whether to include hidden elements on the page. Defaults to `True`.
        mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.

        Returns:
        -------
        dict: Data that matches the natural language prompt.
        """
        start_time = time.time()
        query, accessibility_tree = self._generate_query(
            prompt=prompt,
            timeout=timeout,
            wait_for_network_idle=wait_for_network_idle,
            include_hidden=include_hidden,
            request_origin=kwargs.get("request_origin"),
        )
        elapsed_time = time.time() - start_time
        adjusted_timeout = int(timeout - elapsed_time)
        if adjusted_timeout <= 0:
            raise AgentQLServerTimeoutError()

        response, _ = self._execute_query(
            query=query,
            timeout=adjusted_timeout,
            include_hidden=include_hidden,
            wait_for_network_idle=wait_for_network_idle,
            mode=mode,
            is_data_query=True,
            accessibility_tree=accessibility_tree,
            **kwargs,
        )
        return response

    def query_elements(
        self,
        query: str,
        timeout: int = DEFAULT_QUERY_ELEMENTS_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_ELEMENTS,
        mode: ResponseMode = DEFAULT_RESPONSE_MODE,
        experimental_query_elements_enabled: bool = False,
        **kwargs,
    ) -> AQLResponseProxy:  # type: ignore 'None' warning
        """
        Queries the web page for multiple web elements that match the AgentQL query.

        Parameters:
        ----------
        query (str): An AgentQL query in String format.
        timeout (int) (optional): Timeout value in seconds for the connection with backend API service.
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying the page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        include_hidden (bool) (optional): Whether to include hidden elements on the page. Defaults to `True`.
        mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.
        experimental_query_elements_enabled (bool) (optional): Whether to use the experimental implementation of the query elements feature. Defaults to `False`.

        Returns:
        -------
        AQLResponseProxy: The AgentQL response object with elements that match the query. Response provides access to requested elements via its fields.
        """
        response, query_tree = self._execute_query(
            query=query,
            timeout=timeout,
            include_hidden=include_hidden,
            wait_for_network_idle=wait_for_network_idle,
            mode=mode,
            is_data_query=False,
            experimental_query_elements_enabled=experimental_query_elements_enabled,
            **kwargs,
        )
        return AQLResponseProxy(response, self._page, query_tree)

    def query_data(
        self,
        query: str,
        timeout: int = DEFAULT_QUERY_DATA_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_DATA,
        mode: ResponseMode = DEFAULT_RESPONSE_MODE,
        **kwargs,
    ) -> dict:  # type: ignore 'None' warning
        """
        Queries the web page for data that matches the AgentQL query, such as blocks of text or numbers.

        Parameters:
        ----------
        query (str): An AgentQL query in String format.
        timeout (int) (optional): Timeout value in seconds for the connection with backend API service.
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying the page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        include_hidden (bool) (optional): Whether to include hidden elements on the page. Defaults to `True`.
        mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.

        Returns:
        -------
        dict: Data that matches the query.
        """
        response, _ = self._execute_query(
            query=query,
            timeout=timeout,
            include_hidden=include_hidden,
            wait_for_network_idle=wait_for_network_idle,
            mode=mode,
            is_data_query=True,
            **kwargs,
        )
        return response

    def wait_for_page_ready_state(self, wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE):
        """
        Waits for the page to reach the "Page Ready" state, i.e. page has entered a relatively stable state and most main content is loaded. Might be useful before triggering an AgentQL query or any other interaction for slowly rendering pages.

        Parameters:
        -----------
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        """
        log.debug(f"Waiting for {self._page} to reach 'Page Ready' state")

        # Wait for the page to reach the "Page Ready" state
        determine_load_state_shared(
            page=self._page, monitor=self._page_monitor, wait_for_network_idle=wait_for_network_idle
        )

        # Reset the page monitor after the page is ready
        if self._page_monitor:
            self._page_monitor.reset()

        log.debug(f"Finished waiting for {self._page} to reach 'Page Ready' state")

    def enable_stealth_mode(
        self,
        webgl_vendor: str = VENDOR,
        webgl_renderer: str = RENDERER,
        nav_user_agent: str = USER_AGENT,
        browser_type: Optional[Literal["chrome", "firefox", "safari"]] = "chrome",
    ):
        """
        Enables "stealth mode" with given configuration. To avoid being marked as a bot, parameters' values should match the real values used by your device.
        Use browser fingerprinting websites such as https://bot.sannysoft.com and https://pixelscan.net for realistic examples.

        Parameters:
        -----------
        webgl_vendor (str) (optional):
            The vendor of the GPU used by WebGL to render graphics, such as `Apple Inc.`. After setting this parameter, your browser will emit this vendor information.
        webgl_renderer (str) (optional):
            Identifies the specific GPU model or graphics rendering engine used by WebGL, such as `Apple M3`. After setting this parameter, your browser will emit this renderer information.
        nav_user_agent (str) (optional):
            Identifies the browser, its version, and the operating system, such as `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36`. After setting this parameter, your browser will send this user agent information to the website.
        browser_type (str) (optional):
            The type of the browser. It can be either `chrome`, `firefox`, or `safari`. Defaults to `chrome`. Helps stealth mode to generate browser-specific properties.
        """
        stealth_sync(
            self._page,
            config=StealthConfig(
                vendor=webgl_vendor,
                renderer=webgl_renderer,
                # nav_user_agent will only take effect when navigator_user_agent parameter is True
                nav_user_agent=nav_user_agent,
                navigator_user_agent=nav_user_agent is not None,
                browser_type=BrowserType(browser_type),
            ),
        )

    @experimental_api
    def get_pagination_info(
        self,
        timeout: int = DEFAULT_QUERY_ELEMENTS_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_ELEMENTS,
        mode: ResponseMode = DEFAULT_RESPONSE_MODE,
    ) -> PaginationInfo:  # type: ignore 'None' warning
        """
        Queries the web page for pagination information, for example an element to trigger navigation to the next page.

        Parameters:
        ----------
        timeout (int) (optional): Timeout value in seconds for the connection with backend API service.
        wait_for_network_idle (bool) (optional): Whether to wait for network reaching full idle state before querying the page. If set to `False`, this method will only check for whether page has emitted [`load` event](https://developer.mozilla.org/en-US/docs/Web/API/Window/load_event).
        include_hidden (bool) (optional): Whether to include hidden elements on the page. Defaults to `True`.
        mode (ResponseMode) (optional): The response mode. Can be either `standard` or `fast`. Defaults to `fast`.

        Returns:
        -------
        PaginationInfo: Information related to pagination.
        """
        return PaginationInfo(
            next_page_element=self._get_next_page_element(
                timeout=timeout,
                wait_for_network_idle=wait_for_network_idle,
                include_hidden=include_hidden,
                mode=mode,
            ),
        )

    def get_last_query(self) -> Optional[str]:
        """
        Returns the last query executed by the AgentQL SDK on this page.
        """
        return self._last_query

    def get_last_response(self) -> Optional[dict]:
        """
        Returns the last response generated by the AgentQL server on this page.
        """
        return self._last_response

    def get_last_accessibility_tree(self) -> Optional[dict]:
        """
        Returns the last accessibility tree generated by the AgentQL SDK on this page.
        """
        return self._last_accessibility_tree

    def _get_next_page_element(
        self,
        timeout: int,
        wait_for_network_idle: bool,
        include_hidden: bool,
        mode: ResponseMode,
    ) -> Union[Locator, None]:
        pagination_element = self.get_by_prompt(
            prompt=generate_next_page_element_prompt(),
            timeout=timeout,
            wait_for_network_idle=wait_for_network_idle,
            include_hidden=include_hidden,
            mode=mode,
        )
        return pagination_element

    def _generate_query(
        self,
        prompt: str,
        timeout: int = DEFAULT_QUERY_GENERATE_TIMEOUT_SECONDS,
        wait_for_network_idle: bool = DEFAULT_WAIT_FOR_NETWORK_IDLE,
        include_hidden: bool = DEFAULT_INCLUDE_HIDDEN_DATA,
        request_origin: Optional[str] = None,
    ) -> tuple[str, dict]:
        log.debug(f"Generating query: {prompt}")

        self.wait_for_page_ready_state(wait_for_network_idle=wait_for_network_idle)

        accessibility_tree = get_accessibility_tree(self._page, include_hidden=include_hidden)

        response = generate_query_from_agentql_server(
            prompt, accessibility_tree, timeout, self._page.url, request_origin
        )
        return response, accessibility_tree

    def _execute_query(
        self,
        query: str,
        timeout: int,
        wait_for_network_idle: bool,
        include_hidden: bool,
        mode: ResponseMode,
        is_data_query: bool,
        accessibility_tree: Optional[dict] = None,
        experimental_query_elements_enabled: bool = False,
        **kwargs,
    ) -> tuple[dict, ContainerNode]:
        log.debug(f"Querying {'data' if is_data_query else 'elements'}: {minify_query(query)} on {self._page}")

        query_tree = QueryParser(query).parse()

        self.wait_for_page_ready_state(wait_for_network_idle=wait_for_network_idle)

        if not accessibility_tree:
            accessibility_tree = get_accessibility_tree(self._page, include_hidden=include_hidden)

        log.debug(
            f"AgentQL query execution may take longer than expected, especially for complex queries and lengthy webpages. "
            f"The current timeout is set to {timeout} seconds. If a timeout error occurs, consider extending the timeout."
        )

        response = query_agentql_server(
            query,
            accessibility_tree,
            timeout,
            self._page.url,
            mode,
            is_data_query,
            experimental_query_elements_enabled=experimental_query_elements_enabled,
            **kwargs,
        )

        self._set_debug_info(last_query=query, last_response=response, last_accessibility_tree=accessibility_tree)

        return response, query_tree

    def _set_debug_info(self, last_query: str, last_response: dict, last_accessibility_tree: dict):
        self._last_query = last_query
        self._last_response = last_response
        self._last_accessibility_tree = last_accessibility_tree
