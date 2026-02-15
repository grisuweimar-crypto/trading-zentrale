from pathlib import Path
import re

p = Path(".github/workflows/run_scanner.yml")
t = p.read_text(encoding="utf-8")

# Entferne JEDE kaputt eingefügte Fix-Zeile (egal wie eingerückt)
t = re.sub(r"(?m)^\s*python scripts/fix_contract_cols_post\.py\s*$\n?", "", t)

lines = t.splitlines(True)

# Finde die run_daily Zeile
i_run = None
for i, ln in enumerate(lines):
    if "python -m scanner.app.run_daily" in ln:
        i_run = i
        break
if i_run is None:
    raise SystemExit("❌ run_daily not found in run_scanner.yml")

# Finde den Step-Indent (Zeile darüber mit '- name:')
i_name = None
step_indent = ""
for j in range(i_run, -1, -1):
    m = re.match(r"^(\s*)- name:", lines[j])
    if m:
        i_name = j
        step_indent = m.group(1)
        break
if i_name is None:
    raise SystemExit("❌ Could not find '- name:' above run_daily step")

# Finde Ende dieses Steps (nächster '- name:' auf gleicher Indent-Ebene)
insert_at = None
for k in range(i_name + 1, len(lines)):
    if re.match(rf"^{re.escape(step_indent)}- name:", lines[k]):
        insert_at = k
        break
if insert_at is None:
    insert_at = len(lines)

new_step = (
    f"{step_indent}- name: Fix UI contract cols\n"
    f"{step_indent}  run: python scripts/fix_contract_cols_post.py\n\n"
)

lines.insert(insert_at, new_step)

p.write_text("".join(lines), encoding="utf-8")
print("✅ Patched run_scanner.yml (added separate fix step)")
