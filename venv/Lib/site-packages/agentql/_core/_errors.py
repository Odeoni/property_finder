import json
import logging
import textwrap
from typing import Union

from agentql._core._syntax.node import Node

# pylint: disable-all
AGENTQL_1000_API_KEY_ERROR = 1000
AGENTQL_1001_ATTRIBUTE_NOT_FOUND_ERROR = 1001
AGENTQL_1005_ACCESSIBILITY_TREE_ERROR = 1005
AGENTQL_1006_ELEMENT_NOT_FOUND_ERROR = 1006
AGENTQL_1010_QUERY_SYNTAX_ERROR = 1010
AGENTQL_1012_PAGE_MONITOR_NOT_INITIALIZED_ERROR = 1012
AGENTQL_1013_PAGE_CRASH_ERROR = 1013
AGENTQL_2000_SERVER_ERROR = 2000
AGENTQL_2001_SERVER_TIMEOUT_ERROR = 2001

SUPPORT_MESSAGE = textwrap.dedent(
    """
    If the issue persists, please reach out for further assistance through support@tinyfish.io or Tiny Fish Discord channel https://discord.gg/agentql.
    """
)

REQUEST_ID_MESSAGE = textwrap.dedent(
    """
    Include this Request ID when contacting support for quicker assistance: {request_id}
    """
)

API_KEY_NOT_SET_MESSAGE = textwrap.dedent(
    """
    AgentQL API key is not set.

    Please set the AgentQL API key by invoking `agentql init` command or by setting the environment variable 'AGENTQL_API_KEY'.

    If you don't yet have an AgentQL API key, you can sign up for a free account at https://dev.agentql.com

    You could refer to the documentation for more information: https://docs.agentql.com/installation/sdk-installation.
    """
)

INVALID_API_KEY_MESSAGE = textwrap.dedent(
    """
    Invalid or expired API key provided.

    Please set a valid AgentQL API key by invoking `agentql init` command or by setting the environment variable 'AGENTQL_API_KEY'.

    You could refer to the documentation for more information: https://docs.agentql.com/installation/sdk-installation.
    """
)


log = logging.getLogger("agentql")


class BaseAgentQLError(Exception):
    def __init__(self, error, error_code, request_id=None, include_fallback_message=True):
        self.error = (
            error
            + (SUPPORT_MESSAGE if include_fallback_message else "")
            + (REQUEST_ID_MESSAGE.format(request_id=request_id) if request_id else "")
        )
        self.error_code = error_code
        log.debug(f"Error occurred: {error}")

    def __str__(self):
        return f"{self.error_code} {self.__class__.__name__}: {self.error}"


class APIKeyError(BaseAgentQLError):
    def __init__(
        self,
        message="Invalid or missing API key. Please set the environment variable 'AGENTQL_API_KEY' with a valid API key.",
        request_id=None,
    ):
        super().__init__(
            textwrap.dedent(message),
            AGENTQL_1000_API_KEY_ERROR,
            request_id,
            include_fallback_message=False,
        )


class AttributeNotFoundError(BaseAgentQLError):
    def __init__(self, name: str, response_data: Union[dict, list], query_tree_node: Node):
        if query_tree_node.name:
            response_data = {query_tree_node.name: response_data}
        message = textwrap.dedent(
            f"""
            \"{name}\" not found in AgentQL response node:
            {json.dumps(response_data, indent=2)}
            There could be a few reasons for this:
            1. The element you are trying to access was not part of the original query. Make sure there are no typos in the name of the element you are trying to access.

            Query:
            {query_tree_node.dump()}

            Note that empty spaces and new lines are used to indicate distinct elements. Do verify that you are using empty spaces and new lines correctly in your query. If you want to use multiple words to describe the same element, please use underscores instead. I.e. "search_button".

            2. You may be trying to execute an action on a container node. I.e. the following would raise this error:
            Query:
            {{
                footer {{
                    some_link
                }}
            }}

            response.footer.click()

            In the above example, footer is a container node and you are trying to click on it. You should access the child node instead. I.e. response.footer.some_link.click()
            """
        )
        super().__init__(message, AGENTQL_1001_ATTRIBUTE_NOT_FOUND_ERROR)


class AccessibilityTreeError(BaseAgentQLError):
    def __init__(
        self,
        message=textwrap.dedent(
            """
            An error occurred while generating accessibility tree.
            The page may no longer be available due to navigation, being closed, or crashing.
            """
        ),
    ):
        super().__init__(message, AGENTQL_1005_ACCESSIBILITY_TREE_ERROR)


class ElementNotFoundError(BaseAgentQLError):
    def __init__(self, page_url, element_id=None):
        if element_id:
            message = f"The element with ID {element_id} could not be found on the current page anymore."
        else:
            message = "The element could not be found on the current page anymore."

        message += textwrap.dedent(
            f"""
            The element may have been removed from the page or the page may have been navigated away from.
            The current page url is: {page_url}
            """
        )

        super().__init__(message, AGENTQL_1006_ELEMENT_NOT_FOUND_ERROR)


class QuerySyntaxError(BaseAgentQLError):
    def __init__(self, message=None, *, unexpected_token, row, column):
        if not message:
            message = f"Unexpected character {unexpected_token} at row {row}, column {column} in AgentQL query."
        super().__init__(message, AGENTQL_1010_QUERY_SYNTAX_ERROR, include_fallback_message=False)
        self.unexpected_token = unexpected_token
        self.row = row
        self.column = column


class PageMonitorNotInitializedError(BaseAgentQLError):
    def __init__(
        self,
    ):
        message = textwrap.dedent(
            """
            The page monitor is not initialized.
            Please use the 'page.wait_for_page_ready_state()' method to wait for the page to reach a stable state.
            """
        )
        super().__init__(message, AGENTQL_1012_PAGE_MONITOR_NOT_INITIALIZED_ERROR)


class AgentQLServerError(BaseAgentQLError):
    def __init__(self, error=None, error_code=None, request_id=None):
        if error is None:
            error = "AgentQL Server Error. Please try again."
        if error_code is None:
            error_code = AGENTQL_2000_SERVER_ERROR

        super().__init__(error, error_code, request_id)


class AgentQLServerTimeoutError(AgentQLServerError):
    def __init__(
        self,
        message="The request has timed out because it took longer than the set timeout. Please, consider increasing the timeout value by passing the `timeout` argument to the `get_by_prompt`, `get_data_by_prompt_experimental`, `query_elements` or `query_data` method. For complex requests on lengthy pages or large responses, request can take as much as 15 minutes to process.",
    ):
        super().__init__(message, AGENTQL_2001_SERVER_TIMEOUT_ERROR)


class PageCrashError(BaseAgentQLError):
    def __init__(self, page_url):
        message = textwrap.dedent(
            f"""
            One of the pages have crashed.
            The crashed page url is: {page_url}
            """
        )
        super().__init__(message, AGENTQL_1013_PAGE_CRASH_ERROR)
