from typing import Optional

from .token_kind import TokenKind


class Token:
    """Represents a range of characters within the source."""

    def __init__(
        self,
        kind: TokenKind,
        start: int,
        end: int,
        line: int,
        column: int,
        value: Optional[str] = None,
    ):
        """Initialize the token.

        Parameters:

        kind (TokenKind): The kind of the token.
        start (int): The character offset at which this Node begins.
        end (int): The character offset at which this Node ends.
        line (int): The 1-indexed line number on which this Token appears.
        column (int): The 1-indexed column number at which this Token begins.
        value (str): For non-punctuation tokens, represents the interpreted value of the token.
        """
        self.kind = kind
        self.start = start
        self.end = end
        self.line = line
        self.column = column
        self.value = value
        """The previous token in the token stream."""
        self.prev: Optional[Token] = None
        """The next token in the token stream."""
        self.next: Optional[Token] = None

    def __str__(self):
        return "Token"

    def __repr__(self):
        return (
            f"Token(kind={self.kind}, start={self.start}, end={self.end}, "
            f"line={self.line}, column={self.column}, value={self.value})"
        )

    def to_json(self):
        """Convert the token to a JSON serializable object."""
        return {
            "kind": self.kind.name,  # Use .name to get the name of the Enum member
            "value": self.value,
            "line": self.line,
            "column": self.column,
        }
