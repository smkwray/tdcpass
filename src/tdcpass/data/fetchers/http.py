from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import requests


DEFAULT_TIMEOUT = 30


def download_file(url: str, destination: Path, *, timeout: int = DEFAULT_TIMEOUT) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def get_json(url: str, params: Optional[Dict[str, Any]] = None, *, timeout: int = DEFAULT_TIMEOUT) -> Any:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()
