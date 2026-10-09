# SPDX-License-Identifier: Apache-2.0
"""Offline HTTP smoke: owned process, real middleware, reply/stream/settle and graceful stop."""
import concurrent.futures
import json
import os
import pathlib
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
BASE = "http://127.0.0.1:5181"

def request(path, payload=None):
    req = urllib.request.Request(BASE + path, data=None if payload is None else json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read())

def wait(check, description):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except OSError:
            pass
        time.sleep(0.05)
    raise AssertionError("Timed out: " + description)

environment = {key: value for key, value in os.environ.items()
               if not key.lower().startswith(("legate__", "sample__", "aspnetcore_"))
               and key not in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_API_KEY")}
process = subprocess.Popen(["dotnet", "run", "--project", "samples/CSharpWebHost", "--no-build", "--",
                            "--urls", BASE, "--Sample:Smoke=true", "--Logging:LogLevel:Default=None"], cwd=ROOT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=environment)
try:
    wait(lambda: urllib.request.urlopen(BASE + "/healthz", timeout=2).status == 200, "startup")
    sid = request("/sessions", {"title": "smoke"})["sessionId"]
    answer = request(f"/sessions/{sid}/ask", {"text": "hello"})
    assert answer["assistantText"] == "web sample answer", answer
    with concurrent.futures.ThreadPoolExecutor() as threads:
        settled = threads.submit(request, f"/sessions/{sid}/settle")
        wait(lambda: request("/_sample/waiters") == 1, "waiter registration before prompt")
        request(f"/sessions/{sid}/prompt", {"text": "approve"})
        with urllib.request.urlopen(BASE + f"/sessions/{sid}/events", timeout=30) as stream:
            for raw in stream:
                line = raw.decode().strip()
                if line.startswith("data:"):
                    event = json.loads(line[5:].strip())
                    if event["$type"] == "permissionRequested":
                        request(f"/sessions/{sid}/reply", {"requestId": event["RequestId"]})
                        break
        assert settled.result(timeout=30)["assistantText"] == "web sample answer"
    events = request(f"/sessions/{sid}/journal")
    assert any(e["$type"] == "permissionResolved" for e in events), events
    assert sum(e["$type"] == "turnCompleted" for e in events) == 2, events
    print("CSharpWebHost smoke PASSED: open, ask, stream, permission reply, pre-registered settle")
finally:
    # SIGTERM on Unix, Ctrl+C via the sample smoke-only shutdown endpoint on Windows.
    try:
        request("/_sample/stop", {})
        process.wait(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        process.terminate()
        process.wait(timeout=20)
    if process.returncode != 0:
        raise AssertionError("Sample did not stop cleanly: " + process.stderr.read().decode())
