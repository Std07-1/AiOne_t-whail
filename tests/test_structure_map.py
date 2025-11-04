from __future__ import annotations

from utils.structure_map import build_structure_map


def test_structure_map_distance_flag() -> None:
    stats_near = {"near_edge": True}
    m1 = build_structure_map(stats_near)
    assert m1["distance_to_next_zone"] == 0.0
    assert m1["zone_touch_count"] == 1
    assert m1["last_touch_age"] == 0

    stats_far = {"near_edge": None}
    m2 = build_structure_map(stats_far)
    assert m2["distance_to_next_zone"] is None
    assert m2["zone_touch_count"] == 0
    assert m2["last_touch_age"] is None
