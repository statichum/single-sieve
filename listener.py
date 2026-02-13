import subprocess
import threading
import time
from fastapi import FastAPI, Request
from typing import Set

app = FastAPI()

queue: Set[str] = set()
queue_lock = threading.Lock()
worker_running = False


def worker_loop():
    global worker_running

    while True:
        with queue_lock:
            if not queue:
                worker_running = False
                return

            mbid = queue.pop()

        print(f"[worker] Processing artist MBID: {mbid}", flush=True)

        try:
            subprocess.run(
                ["python", "single-sieve.py", "--artist-mbid", mbid],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"[worker] Error processing {mbid}: {e}", flush=True)


def ensure_worker():
    global worker_running

    with queue_lock:
        if worker_running:
            return
        worker_running = True

    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()


@app.post("/lidarr")
async def lidarr_webhook(req: Request):
    payload = await req.json()

    print(f"[webhook] RAW PAYLOAD: {payload}", flush=True)

    if payload.get("eventType") != "ArtistAdd":
        return {"status": "ignored"}

    artist = payload.get("artist", {})
    mbid = artist.get("mbId")
    name = artist.get("name")

    if not mbid:
        return {"status": "no mbid"}

    with queue_lock:
        if mbid in queue:
            print(f"[webhook] Already queued: {name}", flush=True)
        else:
            queue.add(mbid)
            print(f"[webhook] Enqueued: {name} ({mbid})", flush=True)

    ensure_worker()

    return {"status": "queued"}
