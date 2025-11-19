from enum import Enum


class BrowserProfile(Enum):
    """
    Browser profile presets for different use cases.

    - LIGHT: Basic browser profile with standard configurations
    - STEALTH: Enhanced profile with advanced stealth measures
    """

    LIGHT = "light"
    STEALTH = "stealth"
