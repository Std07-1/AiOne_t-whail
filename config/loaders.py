"""YAML loaders (safe, cached)

Призначення:
- Безпечно читати великі карти з config/data/*.yaml один раз під час старту.
- Не використовувати в гарячому шляху без кешу.

Використання:
- data = load_yaml("symbol_whitelist.yaml")
- thr = load_yaml("stage2_thresholds.yaml")
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DATA_DIR = Path(__file__).with_suffix("").parent / "data"


@lru_cache(maxsize=32)
def load_yaml(name: str) -> dict[str, Any] | list[Any] | None:
    """Завантажити YAML із каталогу config/data.

    Args:
        name: Назва файлу (наприклад, "symbol_whitelist.yaml").

    Returns:
        dict|list|None: Розібрані дані або None, якщо файл не існує/порожній.
    """
    path = DATA_DIR / name
    if not path.exists() or not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data
