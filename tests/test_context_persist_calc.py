from __future__ import annotations

from app import process_asset_batch as pab


def test_compute_ctx_persist_happy() -> None:
    # Буфер із 12 елементів: 6 near_edge істинних, 4 presence >= 0.6
    buf = []
    # Перші 6: near_edge True / "upper", presence високий
    for _ in range(3):
        buf.append({"near_edge": True, "presence": 0.7})
        buf.append({"near_edge": "upper", "presence": 0.65})
    # Наступні 6: near_edge False / None, половина з низьким presence
    for _ in range(3):
        buf.append({"near_edge": False, "presence": 0.4})
        buf.append({"near_edge": None, "presence": 0.2})

    edge_persist, pres_sustain = pab._compute_ctx_persist(buf, 0.6)

    assert edge_persist == 0.5  # 6 з 12 біля краю
    assert pres_sustain == 0.333  # 4 з 12 вище порогу 0.6 округлено до 3 знаків
