# SPDX-License-Identifier: Apache-2.0
"""Sample-only shared-Postgres adapter node-loss evidence, with actual active-call proof."""
import json
import pathlib
import re
import subprocess
import threading
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
NODES = ["http://127.0.0.1:8181", "http://127.0.0.1:8182", "http://127.0.0.1:8183"]

def compose(*args):
    subprocess.run(["docker", "compose", "-f", str(HERE / "compose.yaml"), *args], check=True, timeout=1200)

def request(node, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(node + path, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as response:
        body = response.read()
        return json.loads(body) if body else None

def poll(check, seconds, description):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value is not None and value is not False:
                return value
        except (OSError, ValueError):
            pass
        time.sleep(0.2)  # External harness readiness/observation only, never a runtime timer test.
    raise AssertionError("Timed out: " + description)

def ready(node):
    with urllib.request.urlopen(node + "/ready", timeout=3) as response:
        return response.status == 200

def database_metadata(sid, prime=None):
    assert re.fullmatch(r"[0-9A-Z]{26}", sid)
    if prime is not None:
        assert re.fullmatch(r"[0-9A-Z]{26}", prime)
    condition = "t.turn_id=s.current_turn_id" if prime is None else f"t.turn_id='{prime}'"
    sql = ("SELECT json_build_object('primeTurnId',t.turn_id,'claimOwner',t.claim_owner,'attempt',t.attempt,"
           "'expiresAt',t.claim_expires_at,'status',t.status,'pending',"
           "(SELECT count(*) FROM legate.inbox i WHERE i.session_id=s.id AND i.consumed=FALSE)) "
           f"FROM legate.sessions s JOIN legate.turns t ON {condition} WHERE s.id='{sid}'")
    text = subprocess.check_output(["docker", "compose", "-f", str(HERE / "compose.yaml"), "exec", "-T",
                                    "postgres", "psql", "-U", "legate", "-At", "-c", sql], timeout=20)
    return json.loads(text)

compose("up", "--build", "-d")
try:
    poll(lambda: all(ready(node) for node in NODES), 300, "three-node readiness")
    sid = request(NODES[0], "/sessions", {"title": "adapter recovery"})["sessionId"]
    frames = []
    received = threading.Event()
    def subscribe():
        try:
            with urllib.request.urlopen(NODES[1] + f"/sessions/{sid}/events", timeout=240) as response:
                for raw in response:
                    line = raw.decode().strip()
                    if line.startswith("id:"):
                        frames.append(int(line[3:].strip()))
                        received.set()
        except (OSError, ValueError):
            pass  # Victim may also own the subscriber; durable replay below is authoritative.
    threading.Thread(target=subscribe, daemon=True).start()
    request(NODES[0], f"/sessions/{sid}/prompt", {"text": "hold for actual owner loss"})
    def active_owner():
        for index, node in enumerate(NODES):
            if request(node, "/_sample/active") > 0:
                return index
        return None
    owner = poll(active_owner, 30, "actual provider entry")
    assert received.wait(20), "cross-node subscription never received a sequenced frame"
    survivors = [node for index, node in enumerate(NODES) if index != owner]
    target = request(survivors[0], f"/sessions/{sid}/abort-target")
    assert target is not None, "receiving-node control target missing"
    before = database_metadata(sid)
    assert before["claimOwner"] == f"web-{owner + 1}", "active provider node and durable owner disagree"
    print("victim metadata: " + json.dumps(before, sort_keys=True), flush=True)
    print(f"active provider proved on web-{owner + 1}; received {len(frames)} cross-node frames; killing exact owner", flush=True)
    compose("kill", f"web-{owner + 1}")
    def settled():
        events = request(survivors[0], f"/sessions/{sid}/journal")
        terminals = [e for e in events if e["$type"] in ("turnCompleted", "turnFailed", "turnAborted")]
        return events if terminals else None
    events = poll(settled, 240, "policy-correct recovery settlement")
    terminals = [e for e in events if e["$type"] in ("turnCompleted", "turnFailed", "turnAborted")]
    assert len(terminals) == 1, terminals
    assert terminals[0]["$type"] == "turnCompleted", terminals
    assert terminals[0]["turnId"] == target["turnId"], "recovery changed original real-entry target"
    after = database_metadata(sid, before["primeTurnId"])
    assert after["attempt"] > before["attempt"], "no fenced recovery attempt"
    assert after["status"] == "Completed", after
    print("survivor metadata: " + json.dumps(after, sort_keys=True), flush=True)
    sequences = [e["sequence"] for e in events]
    assert sequences == list(range(1, len(events) + 1)), sequences
    assert frames and all(a <= b for a, b in zip(frames, frames[1:])), frames
    assert set(frames).issubset(set(sequences)), frames
    replay = request(survivors[1], f"/sessions/{sid}/journal")
    assert [e["sequence"] for e in replay] == sequences, "survivor replay differs"
    print(f"adapter node-loss PASSED: owner web-{owner + 1}, {len(events)} gap-free durable sequences, one Completed settlement, identical replay on both survivors", flush=True)
except Exception:
    compose("logs", "--tail", "150", "web-1", "web-2", "web-3")
    compose("exec", "-T", "postgres", "psql", "-U", "legate", "-c",
            "select now(); select id,state,current_turn_id from legate.sessions; select turn_id,status,attempt,claim_owner,claim_expires_at from legate.turns;")
    raise
finally:
    compose("down", "-v")
