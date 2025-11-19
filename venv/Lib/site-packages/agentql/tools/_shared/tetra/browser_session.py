from typing import Any


class BrowserSession:
    """Represents an allocated browser session with CDP connection details."""

    def __init__(self, session_data: dict[str, Any]):
        """
        Initialize BrowserSession with session data from the API.

        Parameters:
        -----------
        session_data: dict[str, Any]
            Raw session data returned from the browser allocation API.

        Raises:
        -------
        ValueError: If required fields are missing from session_data.
        """
        required_fields = ["cdp_url", "base_url"]
        missing_fields = [field for field in required_fields if field not in session_data]

        if missing_fields:
            raise ValueError(f"Missing required fields in session_data: {', '.join(missing_fields)}")

        # Validate field values are not empty
        empty_fields = [field for field in required_fields if not session_data.get(field)]

        if empty_fields:
            raise ValueError(f"Required fields cannot be empty: {', '.join(empty_fields)}")

        # Store specific fields as private attributes
        self._cdp_url = session_data["cdp_url"]
        self._base_url = session_data["base_url"]

    @property
    def cdp_url(self) -> str:
        """
        Get the Chrome DevTools Protocol URL for connecting to the browser.

        Returns:
        --------
        str: The CDP URL for browser connection.
        """
        return self._cdp_url

    def get_page_streaming_url(self, page_num: int) -> str:
        """
        Get the page streaming URL for a specific page number.

        Parameters:
        -----------
        page_num: int
            The page number to get the streaming URL for.

        Returns:
        --------
        str: The streaming URL for the specified page.
        """
        return f"{self._base_url}/stream/{page_num}"
