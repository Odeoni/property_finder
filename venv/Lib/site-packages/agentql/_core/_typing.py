from typing import Literal, TypeVar

InteractiveItemTypeT = TypeVar("InteractiveItemTypeT")
"""
A type variable representing the type of interactive items in a web driver session.
Used in type hints where the exact type depends on the specific web driver library used.
"""

PageTypeT = TypeVar("PageTypeT", covariant=True)  # pylint: disable=typevar-name-incorrect-variance
"""
A type variable representing the type of a page in a web driver session.
Used in type hints where the exact type depends on the specific web driver library used.
"""

BrowserTypeT = TypeVar("BrowserTypeT", covariant=True)
"""
A type variable representing the type of a browser in a web driver session.
Used in type hints where the exact type depends on the specific web driver library used."""

ResponseMode = Literal["standard", "fast"]
"""
A type variable representing the response mode of the AgentQL server.
Used in type hints where the exact type is either 'standard' or 'fast'.
"""
