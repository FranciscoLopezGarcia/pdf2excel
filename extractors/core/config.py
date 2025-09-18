# extractors/core/config_manager.py
import json
from pathlib import Path
from typing import List, Dict, Any

class ConfigManager:
    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        self._cache = {}

    def _load_file(self, path: Path) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def get(self, name: str) -> Dict[str, Any]:
        if not name:
            return self._load_file(self.configs_dir / "default.json")
        if name in self._cache:
            return self._cache[name]
        p = self.configs_dir / f"{name}.json"
        if p.exists():
            cfg = self._load_file(p)
        else:
            cfg = self._load_file(self.configs_dir / "default.json")
        self._cache[name] = cfg
        return cfg

    def load_all_configs(self) -> List[Dict[str, Any]]:
        out = []
        for f in self.configs_dir.glob("*.json"):
            c = self._load_file(f)
            out.append(c)
        return out
