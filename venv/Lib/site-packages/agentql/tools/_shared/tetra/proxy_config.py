from typing import Optional, Union

from pydantic import BaseModel


class TetraProxyConfig(BaseModel):
    """
    Configuration for Tetra's built-in proxy service.

    Attributes:
    -----------
    country_code: Optional[str]
        Two-letter country code for proxy location (e.g., "us", "uk").
        If not specified, a default country will be used.
    """

    country_code: Optional[str] = None

    model_config = {"extra": "forbid"}


class CustomProxyConfig(BaseModel):
    """
    Configuration for custom proxy server.

    Attributes:
    -----------
    url: str
        The proxy server URL (required).
    username: Optional[str]
        Username for proxy authentication (optional).
    password: Optional[str]
        Password for proxy authentication (optional).
    """

    url: str
    username: Optional[str] = None
    password: Optional[str] = None

    model_config = {"extra": "forbid"}


ProxyConfig = Union[TetraProxyConfig, CustomProxyConfig]
