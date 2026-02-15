from pathlib import Path
import re

wf_dir = Path(".github/workflows")
files = list(wf_dir.glob("*.yml")) + list(wf_dir.glob("*.yaml"))
if not files:
    raise SystemExit("❌ No workflow yml/yaml found")

run_daily_pat = re.compile(r"python\s+-m\s+scanner\.app\.run_daily")
ui_gen_pat    = re.compile(r"python\s+-m\s+scanner\.ui\.generator")
fix_line      = "python scripts/fix_contract_cols_post.py"

patched = 0

for p in files:
    txt = p.read_text(encoding="utf-8")

    # already ok?
    if fix_line in txt:
        continue

    # Case 1: one multiline run block contains both commands
    if run_daily_pat.search(txt) and ui_gen_pat.search(txt):
        # Insert fix_line after the first run_daily occurrence inside the same file
        def repl(m):
            return m.group(0) + "\n" + fix_line
        txt2, n = run_daily_pat.subn(repl, txt, count=1)
        if n:
            p.write_text(txt2, encoding="utf-8")
            print(f"✅ patched (inline): {p}")
            patched += 1
            continue

    # Case 2: run_daily exists but ui.generator maybe in other step: add a new step after the line containing run_daily
    if run_daily_pat.search(txt):
        lines = txt.splitlines(True)
        out = []
        inserted = False
        for ln in lines:
            out.append(ln)
            if (not inserted) and run_daily_pat.search(ln):
                indent = ln[:len(ln) - len(ln.lstrip(" "))]
                step = (
                    f"{indent}- name: Fix UI contract cols\n"
                    f"{indent}  run: {fix_line}\n"
                )
                out.append(step)
                inserted = True
        if inserted:
            p.write_text("".join(out), encoding="utf-8")
            print(f"✅ patched (new step): {p}")
            patched += 1

if patched == 0:
    raise SystemExit("❌ Did not patch any workflow. Maybe run_daily is called differently in CI?")
else:
    print(f"✅ total workflows patched: {patched}")
