"""
This module is an entrypoint to AgentQL service
"""

from playwright.sync_api import Page as PlaywrightPage

from agentql.ext.playwright._driver_constants import AGENTQL_PAGE_INSTANCE_KEY
from agentql.ext.playwright.sync_api import Page as AgentQLPage


def wrap(page: PlaywrightPage) -> AgentQLPage:
    """
    Wraps a Playwright Sync `Page` object with an AgentQL `Page` type to get access to the AgentQL's querying API.
    See `agentql.ext.playwright.sync_api.Page` for API details.
    """

    if isinstance(page, AgentQLPage):
        # already wrapped
        return page

    if hasattr(page, AGENTQL_PAGE_INSTANCE_KEY):
        # got non-wrapped page as an input, but it was previously wrapped
        return getattr(page, AGENTQL_PAGE_INSTANCE_KEY)

    agentql_page = AgentQLPage.create(page)
    setattr(page, AGENTQL_PAGE_INSTANCE_KEY, agentql_page)
    return agentql_page
