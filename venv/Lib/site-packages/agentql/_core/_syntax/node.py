from typing import TYPE_CHECKING, Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny

DEFAULT_INDENT = 2


class _NodeWithChildrenMixin:
    if TYPE_CHECKING:
        children: list["Node"]


class Node(BaseModel):
    name: str
    description: Optional[str] = None

    # Freeze the model to make it immutable
    model_config = ConfigDict(frozen=True)

    @property
    def query_name(self) -> str:
        """
        Returns the query name of the node.
        """
        description = f"({self.query_description})" if self.query_description else ""
        return f"{self.name}{description}"

    @property
    def query_description(self) -> Optional[str]:
        """
        Returns the query description of the node.
        """
        return self.description

    def _dump(self, level: int = 0, indent: int = DEFAULT_INDENT) -> str:
        indent_spaces = " " * indent * level
        res = self.query_name
        return f"{indent_spaces}{res}"

    def dump(self, indent: int = DEFAULT_INDENT) -> str:
        """
        Dump the node to a AgentQL Query formatted string.
        """
        return self._dump(level=0, indent=indent)

    def __str__(self) -> str:
        return self.dump()

    def get_child_by_name(self, name):
        """
        Returns the child node with the specified name.
        Raises an AttributeError if no child with the specified name is found.
        """
        if isinstance(self, _NodeWithChildrenMixin):
            for child in self.children:
                if child.name == name:
                    return child
        raise AttributeError(f"Node {self.name} has no child named {name}")

    def get_cache_key(self) -> str:
        """
        Returns the key of the node for caching.
        """
        return f"{self.name}{f'({self.description})' if self.description else ''}"


class IdNode(Node):
    """
    {
        search_btn
    }
    """


class IdListNode(IdNode):
    """
    {
        search_btns[]
    }
    """

    @property
    def query_name(self) -> str:
        description = f"({self.query_description})" if self.query_description else ""
        return f"{self.name}{description}[]"


class ContainerNode(Node, _NodeWithChildrenMixin):
    """
    {
        container {
            child1
            child2
        }
    }
    """

    children: Annotated[SerializeAsAny[list["Node"]], Field(default_factory=list)]

    # pylint: disable=unreachable
    def _dump(self, level: int = 0, indent: int = DEFAULT_INDENT) -> str:
        indent_spaces = " " * indent * level
        node_name = f"{self.query_name} " if self.query_name else ""
        header = f"{indent_spaces}{node_name}{{\n"
        # pylint: disable=protected-access
        body = "\n".join([child._dump(indent=indent, level=level + 1) for child in self.children])
        footer = f"\n{indent_spaces}}}"
        return f"{header}{body}{footer}"


class ContainerListNode(ContainerNode):
    """
    {
        container_list[] {
            child1
            child2
        }
    }
    """

    @property
    def query_name(self) -> str:
        description = f"({self.query_description})" if self.query_description else ""
        return f"{self.name}{description}[]"
