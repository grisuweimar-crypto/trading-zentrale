#!/usr/bin/env python3
"""Browser-level audit of the real ARGUS JS, CSS and embedded rationale JSON.
Runs on a local HTTP server. Does not modify the productive site or research.
"""
from __future__ import annotations
import http.server
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
ARTIFACT=ROOT / "artifacts" / "ui-visual-check"
ARTIFACT.mkdir(parents=True,exist_ok=True)

browser=next((shutil.which(n) for n in ("google-chrome","chromium","chromium-browser") if shutil.which(n)),None)
if not browser:
    raise SystemExit("No Chrome/Chromium browser installed; cannot claim visual smoke test")

class QuietServer(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args): pass

os.chdir(ROOT)
server=http.server.ThreadingHTTPServer(("127.0.0.1",0),QuietServer)
threading.Thread(target=server.serve_forever,daemon=True).start()
base=f"http://127.0.0.1:{server.server_port}"
fixture=base+"/tests/fixtures/argus_editorial_preview.html"
research=base+"/argus-preview/v7/"
opts=["--headless","--no-sandbox","--disable-gpu","--disable-dev-shm-usage",
      "--hide-scrollbars","--allow-insecure-localhost","--virtual-time-budget=19000",
      "--disable-background-networking","--window-size=1440,1000"]
try:
    urllib.request.urlopen(fixture,timeout=10).read(60)
    dump=subprocess.run([browser,*opts,"--dump-dom",fixture],capture_output=True,text=True,timeout=110)
    if dump.returncode != 0:
        raise AssertionError("Chromium failed to load candidate UI: "+dump.stderr[-800:])
    html=dump.stdout
    missing=[x for x in (
        "Infineon","Warum wurde die Aktie vorgeschlagen?",
        "Eaton","Gewinnmitnahmen nach starkem Kursanstieg","65 EUR",
        "Lululemon Athletica","Früherer Scannervergleich"
    ) if x not in html]
    if missing:
        raise AssertionError("Real browser did not render: "+", ".join(missing)+
                             "\nTail: "+html[-1000:]+ "\nChrome errors: "+dump.stderr[-550:])
    first=html.index("Infineon")
    last=html.find('<article class="etwCard"',first)
    card=html[first:last if last>0 else first+12000]
    assert card.index("Gewinnmitnahmen nach starkem Kursanstieg") < card.index("Früherer Scannervergleich"), "Infineon rationale still buried"
    assert 'argus-editorial.css' in html, "Editorial style not requested"

    for label,url,width,height in (
        ("beobachtungen-desktop",fixture,1440,1000),
        ("beobachtungen-mobil",fixture,390,844),
        ("forschungsstand-desktop",research,1440,1000),
        ("forschungsstand-mobil",research,390,844),
    ):
        output=ARTIFACT/(label+".png")
        shot=subprocess.run([browser,*[a for a in opts if not a.startswith("--window-size")],
                             f"--window-size={width},{height}",f"--screenshot={output}",url],
                            capture_output=True,text=True,timeout=110)
        if shot.returncode!=0 or not output.exists() or output.stat().st_size<2000:
            raise AssertionError(label+" screenshot missing. "+shot.stderr[-600:])
        print(label,output.stat().st_size,"bytes")
    # The actual 1+ MB main page, not the smaller candidate-only fixture, must work.
    # The iframe shim switches tabs/modes and checks the visible active nav, exactly
    # one footer and a loaded replacement image without changing scanner internals.
    shell=base+"/tests/fixtures/argus_shell_acceptance.html"
    for label,mode,width,height in (
        ("about-dark-desktop","view=about",1440,1000),
        ("about-dark-mobile","view=about&mobile=1",390,844),
        ("start-dark-mobile","view=start&mobile=1",390,844),
        ("scanner-dark-mobile","view=research&mobile=1",390,844),
        ("beobachtungen-dark-mobile","view=observations&mobile=1",390,844),
        ("forschungsstand-dark-mobile","view=science&mobile=1",390,844),
    ):
        url=shell+"?"+mode
        dom=subprocess.run([browser,*opts,"--dump-dom",url],
                           capture_output=True,text=True,timeout=120)
        if dom.returncode!=0 or 'id="argus-test-status">ARGUS_UI_OK</div>' not in dom.stdout:
            summary=dom.stdout[-2200:].replace("ARGUS_UI_PENDING","PENDING")
            raise AssertionError(label+" did not complete real-browser dark-only/original-art/footer checks: "+
                                 summary+"; "+dom.stderr[-500:])
        output=ARTIFACT/(label+".png")
        shot=subprocess.run([browser,*[a for a in opts if not a.startswith("--window-size")],
                             f"--window-size={width},{height}",f"--screenshot={output}",url],
                            capture_output=True,text=True,timeout=120)
        if shot.returncode!=0 or not output.exists() or output.stat().st_size<2000:
            raise AssertionError(label+" screenshot missing. "+shot.stderr[-500:])
        print(label,output.stat().st_size,"bytes; DOM checks passed")
    print("PASS: real Chrome renders historical and dated trading rationales before archive notes.")
finally:
    server.shutdown()
