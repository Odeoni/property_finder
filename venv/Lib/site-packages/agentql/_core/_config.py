import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class Config:
    """
    A class to hold configuration options for the agentql library.
    """

    def __init__(self) -> None:
        self.api_key: Optional[str] = None

    def update(
        self,
        api_key: Optional[str] = None,
    ) -> None:
        if api_key:
            self.api_key = api_key

    def get_api_key(self) -> Optional[str]:
        return self.api_key or os.getenv("AGENTQL_API_KEY")


config = Config()


def configure(
    *,
    api_key: Optional[str] = None,
) -> None:
    """
    Configure the agentql library with specified options.

    Parameters
    ----------
    api_key : Optional[str], optional
        Your API key for authentication. Default is None.
    """
    config.update(api_key=api_key)
