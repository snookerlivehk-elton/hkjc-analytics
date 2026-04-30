from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class SettlementPlugin(Protocol):
    plugin_key: str

    def settle(
        self,
        *,
        race_id: int,
        pred_top5: List[int],
        actual_top5: List[int],
        dividends: Optional[List[Dict[str, Any]]],
        settled_at: str,
    ) -> Optional[Dict[str, Any]]: ...


_PLUGINS: List[SettlementPlugin] = []


def register(plugin: SettlementPlugin) -> None:
    _PLUGINS.append(plugin)


def get_plugins() -> List[SettlementPlugin]:
    return list(_PLUGINS)

