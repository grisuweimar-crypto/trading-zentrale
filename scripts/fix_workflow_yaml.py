from pathlib import Path
import re

p = Path(".github/workflows/run_scanner.yml")
txt = p.read_text(encoding="utf-8")
lines = txt.splitlines(True)

# Find the run_daily line
i_run = None
for i, ln in enumerate(lines):
    if "python -m scanner.app.run_daily" in ln:
        i_run = i
        break
if i_run is None:
    raise SystemExit("❌ Could not find run_daily in run_scanner.yml")

# Find step start ("- name:") above run_daily
i_step = None
step_indent = ""
for j in range(i_run, -1, -1):
    m = re.match(r"^(\s*)- name:", lines[j])
    if m:
        i_step = j
        step_indent = m.group(1)
        break
if i_step is None:
    raise SystemExit("❌ Could not find step header above run_daily")

# Find the "run:" line for this step
i_runkey = None
run_indent = ""
for j in range(i_run, i_step, -1):
    if re.match(r"^\s*run:\s*\|", lines[j]) or re.match(r"^\s*run:\s*", lines[j]):
        i_runkey = j
        run_indent = re.match(r"^(\s*)", lines[j]).group(1)
        break
if i_runkey is None:
    raise SystemExit("❌ Could not find run: line for run_daily step")

# Remove any previously inserted broken fix lines (unindented python scripts/...)
lines2 = []
for ln in lines:
    if ln.strip() == "python scripts/fix_contract_cols_post.py":
        continue
    lines2.append(ln)
lines = lines2

# Refresh indexes after cleanup
txt = "".join(lines)
lines = txt.splitlines(True)
i_run = next(i for i, ln in enumerate(lines) if "python -m scanner.app.run_daily" in ln)
i_step = next(i for i in range(i_run, -1, -1) if re.match(r"^(\s*)- name:", lines[i]))
step_indent = re.match(r"^(\s*)- name:", lines[i_step]).group(1)

# Convert this step to single-line run (no block), to avoid YAML indentation issues
# If it is a run: | block, we keep only run_daily and remove the rest of the block.
i_runkey = None
for j in range(i_run, i_step, -1):
    if re.match(r"^\s*run:\s*\|", lines[j]) or re.match(r"^\s*run:\s*", lines[j]):
        i_runkey = j
        run_indent = re.match(r"^(\s*)", lines[j]).group(1)
        break

if re.match(r"^\s*run:\s*\|", lines[i_runkey]):
    # rewrite run line
    lines[i_runkey] = f"{run_indent}run: python -m scanner.app.run_daily\n"
    # delete following block lines that are more indented than run_indent
    k = i_runkey + 1
    while k < len(lines):
        ln = lines[k]
        if ln.strip() == "":
            # keep blank lines
            k += 1
            continue
        indent = re.match(r"^(\s*)", ln).group(1)
        if len(indent) <= len(run_indent):
            break
        # remove block content line
        lines[k] = ""
        k += 1

# Insert a new step right after the run_daily step (before next "- name:")
# Find insertion point: next step at same indent
insert_at = None
for k in range(i_step + 1, len(lines)):
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

p.write_text("".join([ln for ln in lines if ln is not None]), encoding="utf-8")
print("✅ Fixed YAML + added separate fix step safely")
