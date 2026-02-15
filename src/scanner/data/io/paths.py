from __future__ import annotations
from pathlib import Path

def project_root() -> Path:
    """
    Findet den Projektroot robust, indem nach einem Marker gesucht wird.
    Marker: run.py + src/
    Funktioniert unabhängig vom Working Directory (Spyder/OneDrive/Terminal).
    """
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "run.py").exists() and (p / "src").is_dir():
            return p
    # Fallback: wenn Marker nicht gefunden wird, nimm die Eltern von src/
    # (sollte in deinem Setup praktisch nie greifen)
    return here.parents[5]

def artifacts_dir() -> Path:
    p = project_root() / "artifacts"
    p.mkdir(parents=True, exist_ok=True)
    return p
