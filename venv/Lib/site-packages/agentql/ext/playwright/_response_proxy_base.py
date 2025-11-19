import json
import logging
from typing import Generic, Union

from typing_extensions import Self

from agentql import AttributeNotFoundError, ContainerNode, InteractiveItemTypeT

log = logging.getLogger("agentql")


class BaseAQLResponseProxy(Generic[InteractiveItemTypeT]):
    _response_data: Union[dict, list]
    _query_tree_node: ContainerNode

    def __getattr__(self: Self, name) -> Union[InteractiveItemTypeT, Self]:
        if self._response_data is None:
            raise AttributeError("Response data is None")
        if name not in self._response_data:
            raise AttributeNotFoundError(name, self._response_data, query_tree_node=self._query_tree_node)
        log.debug(f"Resolving element {name}")
        return self._resolve_item(self._response_data[name], self._query_tree_node.get_child_by_name(name))  # type: ignore # returned value could be None, but to make static checker happy we ignore it

    def __getitem__(self: Self, index: int) -> Union[InteractiveItemTypeT, Self]:
        if not isinstance(self._response_data, list):
            raise ValueError("This node is not a list")
        return self._resolve_item(self._response_data[index], self._query_tree_node)  # type: ignore # returned value could be None, but to make static checker happy we ignore it

    def __len__(self):
        if self._response_data is None:
            return 0
        return len(self._response_data)

    def __str__(self):
        return json.dumps(self._response_data, indent=2)

    def __iter__(self):
        if not isinstance(self._response_data, list):
            raise ValueError("This node is not a list")

        for item in self._response_data:
            yield self._resolve_item(item, self._query_tree_node)  # type: ignore
