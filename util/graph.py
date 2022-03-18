from collections import deque
from typing import Callable, Deque, Generic, Iterable, Optional, TypeVar

T = TypeVar("T")

class Graph(Generic[T]):
    def __init__(self, nodes: list[T], explorer: Callable[[T], Iterable[T]]):
        self.nodes = nodes
        self.explorer = explorer

    def reachable_nodes_from(self, nodes: Iterable[T], validator: Callable[[T, Optional[T]], bool]):
        explored: set[T] = set()
        to_explore: Deque[tuple[T, Optional[T]]] = deque((n, None) for n in nodes)
        while len(to_explore) > 0:
            current, origin = to_explore.popleft()
            yield current
            if current not in explored:
                explored.add(current)
                if validator(current, origin):
                    to_explore.extend(((n, current) for n in self.explorer(current)))

    def sink_to_source_exploration(self):
        explored: set[T] = set()
        not_explored = set(self.nodes)
        while len(not_explored) > 0:
            for node in list(not_explored):
                if explored.issuperset(self.explorer(node)):
                    yield node
                    explored.add(node)
                    not_explored.remove(node)
                    break
            else:
                raise Exception("Graph contains a cycle")



                

