from enum import Enum


class UserAgentPreset(Enum):
    """
    User Agent presets for different operating systems. By selecting specific preset underlying remote browser
    will mimic user agent of the selected operating system modifying HTTP headers, navigator object and more.
    """

    WINDOWS = "windows"
    MACOS = "macos"
    LINUX = "linux"
