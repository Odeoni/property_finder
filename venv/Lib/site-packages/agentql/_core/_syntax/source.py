from typing import Optional


class Location:
    def __init__(self, line: int = 1, column: int = 1):
        self.line = line
        self.column = column


class Source:
    """A representation of source input to AgentQL. The `name` and `locationOffset` parameters are
    optional, but they are useful for clients who store AgentQL queries in source files.
    For example, if the AgentQL input starts at line 40 in a file named `daily_oil_price.agentql`, it might
    be useful for `name` to be `"daily_oil_price.agentql"` and location to be `{ line: 40, column: 1 }`.
    The `line` and `column` properties in `locationOffset` are 1-indexed.
    """

    def __init__(
        self,
        body: str,
        name: str = "AgentQL query request",
        location_offset: Optional[Location] = None,
    ):
        """Initialize the source.

        Parameters:

        body (str): The actual query string.
        name (str): The name of the source.
        location_offset (Location): The location offset of the source.
        """
        self.body = body
        self.name = name
        self.location_offset = location_offset or Location()

    def __str__(self):
        return "Source"
