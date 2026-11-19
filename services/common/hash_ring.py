import bisect
import hashlib
from typing import Dict, Iterable, List, Optional, Set
from services.common.config import settings


class ConsistentHashRing:
    def __init__(self, virtual_nodes: Optional[int] = None):
        self.virtual_nodes: int = virtual_nodes or settings.HASH_RING_VIRTUAL_NODES
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        # 128-bit MD5 digest converted to integer for uniform distribution
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.virtual_nodes):
            vkey = f"{node}#vnode{i}"
            h = self._hash(vkey)
            self._ring[h] = node
            bisect.insort(self._sorted_keys, h)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.remove(node)
        # Rebuild ring keys without the removed node
        self._ring = {h: n for h, n in self._ring.items() if n != node}
        self._sorted_keys = sorted(self._ring.keys())

    def set_nodes(self, nodes: Iterable[str]) -> None:
        target_nodes = set(nodes)
        to_remove = self._nodes - target_nodes
        to_add = target_nodes - self._nodes

        for node in to_remove:
            self.remove_node(node)
        for node in to_add:
            self.add_node(node)

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_right(self._sorted_keys, h)
        if idx == len(self._sorted_keys):
            idx = 0
        return self._ring[self._sorted_keys[idx]]

    def get_nodes(self) -> Set[str]:
        return set(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)
