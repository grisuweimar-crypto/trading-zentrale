# ZIP-Hinweis

- FULL-ZIP: komplettes Projekt (pyproject.toml + src + scripts + configs). In leeren Ordner entpacken.
- PATCH-ZIP: nur geaenderte Dateien (haeufig nur src/). Immer ueber ein bestehendes Projekt entpacken.

Wenn `pip install -e .` meldet, dass kein pyproject.toml da ist, bist du im falschen Ordner oder hast nur eine PATCH-ZIP entpackt.
