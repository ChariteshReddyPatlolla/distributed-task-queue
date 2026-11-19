import pytest
from services.common.hash_ring import ConsistentHashRing


def test_empty_ring():
    ring = ConsistentHashRing(virtual_nodes=50)
    assert ring.get_node("any-key") is None
    assert len(ring) == 0


def test_deterministic_routing():
    ring = ConsistentHashRing(virtual_nodes=100)
    workers = ["worker-1", "worker-2", "worker-3"]
    for w in workers:
        ring.add_node(w)

    key = "job-uuid-12345"
    target = ring.get_node(key)
    assert target in workers

    # Calling get_node multiple times must yield the identical node
    for _ in range(50):
        assert ring.get_node(key) == target


def test_uniform_distribution():
    ring = ConsistentHashRing(virtual_nodes=100)
    workers = ["worker-a", "worker-b", "worker-c"]
    for w in workers:
        ring.add_node(w)

    key_counts = {w: 0 for w in workers}
    total_keys = 3000

    for i in range(total_keys):
        node = ring.get_node(f"job-key-{i}")
        assert node is not None
        key_counts[node] += 1

    # In a 3-node ring with 100 virtual nodes each, expected share is ~33.3%
    # Each node should get between 20% and 45% of the total keys
    for w, count in key_counts.items():
        share = count / total_keys
        assert 0.20 <= share <= 0.45, f"Worker {w} received {share:.2%} of keys, outside balanced range"


def test_minimal_remapping_on_node_addition():
    ring = ConsistentHashRing(virtual_nodes=100)
    initial_workers = ["w1", "w2", "w3"]
    for w in initial_workers:
        ring.add_node(w)

    total_keys = 1000
    initial_mapping = {f"k{i}": ring.get_node(f"k{i}") for i in range(total_keys)}

    # Add 4th node
    ring.add_node("w4")
    new_mapping = {f"k{i}": ring.get_node(f"k{i}") for i in range(total_keys)}

    remapped_count = 0
    for k in initial_mapping:
        old_node = initial_mapping[k]
        new_node = new_mapping[k]
        if old_node != new_node:
            remapped_count += 1
            # Any remapped key must have moved to the new node 'w4'
            assert new_node == "w4", f"Key {k} remapped from {old_node} to {new_node} instead of w4"

    # Remapping proportion should be roughly 1/(N+1) = 25% (allow range 15% - 40%)
    remap_ratio = remapped_count / total_keys
    assert 0.15 <= remap_ratio <= 0.40, f"Remap ratio was {remap_ratio:.2%}"


def test_minimal_remapping_on_node_removal():
    ring = ConsistentHashRing(virtual_nodes=100)
    workers = ["w1", "w2", "w3", "w4"]
    for w in workers:
        ring.add_node(w)

    total_keys = 1000
    initial_mapping = {f"k{i}": ring.get_node(f"k{i}") for i in range(total_keys)}

    # Remove w4
    ring.remove_node("w4")
    new_mapping = {f"k{i}": ring.get_node(f"k{i}") for i in range(total_keys)}

    for k in initial_mapping:
        old_node = initial_mapping[k]
        new_node = new_mapping[k]
        if old_node == "w4":
            assert new_node in ["w1", "w2", "w3"]
        else:
            # Keys on surviving nodes should remain on the exact same node
            assert new_node == old_node, f"Key {k} on surviving node {old_node} moved to {new_node}"
