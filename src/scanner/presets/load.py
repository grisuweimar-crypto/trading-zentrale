from __future__ import annotations
import json
import os
from pathlib import Path

def load_presets() -> dict:
    # 1) optional: override via env var (praktisch für Experimente)
    env = os.getenv("SCANNER_PRESETS_PATH")
    candidates: list[Path] = []
    if env:
        candidates.append(Path(env))

    # 2) default: neben dieser Datei
    candidates.append(Path(__file__).with_name("presets.json"))

    # 3) fallback: project root (falls du später umziehst)
    candidates.append(Path(__file__).resolve().parents[3] / "presets.json")

    for path in candidates:
        if path.exists():
            try:
                text = path.read_text(encoding="utf-8-sig")  # BOM-safe
                data = json.loads(text)
            except Exception as e:
                raise RuntimeError(f"Presets konnten nicht geladen werden: {path}\n{e}") from e

            if not isinstance(data, dict):
                raise TypeError(f"presets.json muss ein dict/object sein, got: {type(data).__name__} ({path})")

            return data

    looked = "\n".join(str(p) for p in candidates)
    raise FileNotFoundError("presets.json nicht gefunden. Gesucht in:\n" + looked)
