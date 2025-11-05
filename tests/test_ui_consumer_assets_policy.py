import pytest

from UI.ui_consumer import UIConsumer


def test_apply_assets_update_list_replaces_display():
    ui = UIConsumer()
    initial = [{"symbol": "BTCUSDT"}]
    ui._display_results = initial.copy()
    ui._last_results = initial.copy()

    new_assets = [{"symbol": "ETHUSDT"}]
    ui._apply_assets_update(new_assets)

    assert ui._last_results == new_assets
    assert ui._display_results == new_assets


def test_apply_assets_update_non_list_clears_results():
    ui = UIConsumer()
    ui._display_results = [{"symbol": "BTCUSDT"}]
    ui._last_results = [{"symbol": "BTCUSDT"}]

    # Non-list should clear
    ui._apply_assets_update(None)
    assert ui._last_results == []
    assert ui._display_results == []

    # Empty dict should also clear
    ui._display_results = [{"symbol": "BTCUSDT"}]
    ui._last_results = [{"symbol": "BTCUSDT"}]
    ui._apply_assets_update({})
    assert ui._last_results == []
    assert ui._display_results == []
