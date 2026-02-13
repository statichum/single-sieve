#!/usr/bin/env python3

import requests
import time
import yaml
import sys
import re
import signal
import argparse

# ------------------------------------------------------------
# Globals for graceful shutdown
# ------------------------------------------------------------
shutdown_requested = False


def handle_sigint(signum, frame):
    global shutdown_requested
    if not shutdown_requested:
        shutdown_requested = True
        print("\nCtrl+C received — will finish current artist, then exit.", flush=True)
    else:
        print("\nCtrl+C again — exiting immediately.", flush=True)
        raise KeyboardInterrupt


signal.signal(signal.SIGINT, handle_sigint)

# ------------------------------------------------------------
# Config + helpers
# ------------------------------------------------------------

def load_config(path="config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
def parse_args():
    parser = argparse.ArgumentParser(description="Single-Sieve for Lidarr")
    parser.add_argument(
        "--artist-mbid",
        help="Process a single artist by MusicBrainz ID (overrides config scope)",
    )
    parser.add_argument(
        "--artist-name",
        help="Process a single artist by exact Lidarr artistName (overrides config scope)",
    )
    return parser.parse_args()


def lidarr_get(cfg, path, params=None):
    r = requests.get(
        f"{cfg['lidarr']['url']}/api/v1/{path}",
        headers={"X-Api-Key": cfg["lidarr"]["api_key"]},
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def lidarr_put(cfg, path, payload):
    r = requests.put(
        f"{cfg['lidarr']['url']}/api/v1/{path}",
        headers={
            "X-Api-Key": cfg["lidarr"]["api_key"],
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    # Lidarr often returns the updated object
    return r.json()


def lidarr_post(cfg, path, payload):
    r = requests.post(
        f"{cfg['lidarr']['url']}/api/v1/{path}",
        headers={
            "X-Api-Key": cfg["lidarr"]["api_key"],
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def normalise(title, rules):
    s = title or ""

    if rules.get("lowercase"):
        s = s.lower()

    s = s.replace("&", "and")

    if rules.get("strip_parentheses"):
        s = re.sub(r"\([^)]*\)", "", s)

    if rules.get("strip_brackets"):
        s = re.sub(r"\[[^]]*\]", "", s)

    if rules.get("remove_punctuation"):
        s = re.sub(r"[^a-z0-9\s]", "", s)

    if rules.get("collapse_whitespace"):
        s = re.sub(r"\s+", " ", s)

    return s.strip()


# ------------------------------------------------------------
# Lidarr command helpers
# ------------------------------------------------------------

def wait_for_command(cfg, cmd_id, *, label="command", poll_seconds=1):
    while True:
        status = lidarr_get(cfg, f"command/{cmd_id}")
        state = status.get("status")

        if state == "completed":
            return
        if state == "failed":
            msg = status.get("message") or ""
            raise RuntimeError(f"{label} failed (id={cmd_id}). {msg}".strip())

        time.sleep(poll_seconds)


def refresh_artist_and_wait(cfg, artist_id):
    """
    Trigger RefreshArtist and block until Lidarr reports completion.
    """
    cmd = lidarr_post(cfg, "command", {
        "name": "RefreshArtist",
        "artistId": artist_id,
    })

    cmd_id = cmd.get("id")
    if not cmd_id:
        raise RuntimeError("Failed to get RefreshArtist command ID")

    wait_for_command(cfg, cmd_id, label=f"RefreshArtist artistId={artist_id}")


def artist_search(cfg, artist_id):
    """
    Trigger Lidarr's artist-wide search (this matches the UI "Search Monitored" intent).
    """
    cmd = lidarr_post(cfg, "command", {
        "name": "ArtistSearch",
        "artistId": artist_id,
    })
    return cmd.get("id")


# ------------------------------------------------------------
# Monitoring helpers
# ------------------------------------------------------------

def set_artist_monitored(cfg, artist_obj, monitored, *, dry_run=False):
    """
    PUT the full artist object with monitored toggled.
    Returns the updated artist object (or the original if dry-run).
    """
    current = bool(artist_obj.get("monitored", False))
    if current == monitored:
        return artist_obj

    artist_obj["monitored"] = monitored

    if dry_run:
        print(f"  [DRY] set artist monitored={monitored}", flush=True)
        return artist_obj

    updated = lidarr_put(cfg, "artist", artist_obj)
    return updated


# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------

def main():
    args = parse_args()
    cfg = load_config()

    behaviour = cfg.get("behaviour", {})
    dry_run = behaviour.get("dry_run", False)
    apply_profile = behaviour.get("apply_profile", True)
    suppress_duplicates = behaviour.get("suppress_duplicates", True)
    debug_kept = behaviour.get("debug_kept", True)
    debug_kept_limit = int(behaviour.get("debug_kept_limit", 8))
    skip_unmonitored = behaviour.get("skip_unmonitored_artists", False)

    # NEW
    search_monitored = bool(behaviour.get("search_monitored", False))

    print("Loading artists from Lidarr…")
    artists = lidarr_get(cfg, "artist")

    # ------------------------------------------------------------
    # CLI override mode (single artist)
    # ------------------------------------------------------------
    if args.artist_mbid or args.artist_name:
        selected = []

        if args.artist_mbid:
            selected = [
                a for a in artists
                if a.get("foreignArtistId") == args.artist_mbid
            ]
            if not selected:
                print(f"Artist not found for MBID: {args.artist_mbid}")
                sys.exit(1)

        elif args.artist_name:
            selected = [
                a for a in artists
                if a.get("artistName") == args.artist_name
            ]
            if not selected:
                print(f"Artist not found for name: {args.artist_name}")
                sys.exit(1)

        artists = selected
        print(f"CLI override: processing 1 artist → {artists[0].get('artistName')}")

    # ------------------------------------------------------------
    # Config scope mode
    # ------------------------------------------------------------
    else:
        artists_cfg = [
            a.strip().lower()
            for a in cfg["scope"].get("artists", [])
            if a.strip()
        ]
        full_library = cfg["scope"].get("process_full_library", False)

        if not artists_cfg and not full_library:
            print("Nothing to do: no artists listed and full library disabled")
            sys.exit(1)

        if artists_cfg:
            artists = [
                a for a in artists
                if a.get("artistName", "").lower() in artists_cfg
            ]

    profiles = lidarr_get(cfg, "metadataProfile")
    profile_by_name = {p["name"]: p["id"] for p in profiles}

    target_profile = cfg["metadata"]["target_profile"]
    ignore_profiles = set(cfg["metadata"].get("ignore_profiles", []))

    if target_profile not in profile_by_name:
        print(f"Metadata profile not found: {target_profile}")
        sys.exit(1)

    target_profile_id = profile_by_name[target_profile]

    for artist in artists:
        name = artist.get("artistName", "Unknown")
        artist_id = artist.get("id")
        was_monitored = bool(artist.get("monitored", False))

        current_profile = next(
            (p["name"] for p in profiles if p["id"] == artist.get("metadataProfileId")),
            "unknown",
        )

        needs_profile_change = (artist.get("metadataProfileId") != target_profile_id)

        print(f"\n{name}")
        print(f"  current profile: {current_profile}")

        if skip_unmonitored and not was_monitored:
            print("  skipping (artist unmonitored)")
            if shutdown_requested:
                print("Shutdown requested — stopping before next artist.")
                break
            continue

        if current_profile in ignore_profiles:
            print("  skipping (ignored profile)")
            if shutdown_requested:
                print("Shutdown requested — stopping before next artist.")
                break
            continue

        # ------------------------------------------------------------
        # NEW ORDER: unmonitor first (only if originally monitored)
        # ------------------------------------------------------------
        try:
            if was_monitored and needs_profile_change:
                print("  temporarily unmonitoring artist…")
                artist = set_artist_monitored(cfg, artist, False, dry_run=dry_run)


            # Apply metadata profile (while unmonitored to avoid race/search)
            if apply_profile and artist.get("metadataProfileId") != target_profile_id:
                print(f"  setting profile → {target_profile}")
                if dry_run:
                    print("  [DRY] PUT /artist (metadataProfileId change)")
                    artist["metadataProfileId"] = target_profile_id
                else:
                    artist["metadataProfileId"] = target_profile_id
                    artist = lidarr_put(cfg, "artist", artist)

            # Refresh and wait deterministically
            if needs_profile_change:
                print("  refreshing artist metadata…")
                if not dry_run:
                    refresh_artist_and_wait(cfg, artist_id)
                else:
                    print("  [DRY] RefreshArtist")


            cooldown = cfg.get("timing", {}).get("post_refresh_cooldown_seconds", 0)
            if cooldown and cooldown > 0:
                time.sleep(cooldown)

            # ------------------------------------------------------------
            # Singles prune
            # ------------------------------------------------------------
            tracks = lidarr_get(cfg, "track", {"artistId": artist_id})
            albums = lidarr_get(cfg, "album", {"artistId": artist_id})

            singles = [a for a in albums if a.get("albumType") == "Single"]
            monitored_non_single_release_ids = {
                a["id"]
                for a in albums
                if a.get("albumType") != "Single" and a.get("monitored")
            }


            seen_titles_elsewhere = set()
            seen_recordings_elsewhere = set()

            for t in tracks:
                if t.get("albumId") in monitored_non_single_release_ids:
                    title = t.get("title")
                    if title:
                        seen_titles_elsewhere.add(
                            normalise(title, cfg["normalisation"])
                        )

                    recording_id = t.get("recordingId")
                    if recording_id:
                        seen_recordings_elsewhere.add(recording_id)


            tracks_by_album = {}
            for t in tracks:
                tracks_by_album.setdefault(t.get("albumId"), []).append(t)

            suppressed = []
            kept = []
            debug_misses = {}

            for s in singles:
                s_id = s["id"]
                s_title = s.get("title", "Unknown Single")

                s_tracks = tracks_by_album.get(s_id, [])
                if not s_tracks:
                    kept.append(s_title)
                    debug_misses[s_title] = ["<no tracks visible>"]
                    continue

                missing = []

                for t in s_tracks:
                    title = t.get("title")
                    if not title:
                        continue

                    norm_title = normalise(title, cfg["normalisation"])
                    recording_id = t.get("recordingId")

                    title_match = norm_title in seen_titles_elsewhere
                    recording_match = (
                        recording_id is not None and
                        recording_id in seen_recordings_elsewhere
                    )

                    if not (title_match or recording_match):
                        missing.append(norm_title)

                if not missing:
                    suppressed.append(s_title)
                    if suppress_duplicates:
                        if dry_run:
                            print(f"  [DRY] unmonitor single: {s_title}")
                        else:
                            album_detail = lidarr_get(cfg, f"album/{s_id}")
                            album_detail["monitored"] = False
                            lidarr_put(cfg, "album", album_detail)
                else:
                    kept.append(s_title)
                    debug_misses[s_title] = missing

            print(f"  singles found: {len(singles)}")
            print(f"  suppressed duplicates: {len(suppressed)}")
            for t in suppressed:
                print(f"    - {t}")
            print(f"  kept: {len(kept)}")

            if debug_kept and kept:
                for title in kept[:debug_kept_limit]:
                    print(f"  (debug) kept: {title}")
                    for m in debug_misses.get(title, [])[:5]:
                        print(f"    missing: {m}")

            # ------------------------------------------------------------
            # NEW: re-monitor artist (restore state) then optional search
            # ------------------------------------------------------------
            if was_monitored and needs_profile_change:
                print("  re-monitoring artist…")
                artist = set_artist_monitored(cfg, artist, True, dry_run=dry_run)

                if search_monitored:
                    print("  triggering search monitored…")
                    if dry_run:
                        print("  [DRY] ArtistSearch")
                    else:
                        cmd_id = artist_search(cfg, artist_id)
                        if cmd_id:
                            print(f"  search command queued (id={cmd_id})")




        finally:
            # Safety net: if something throws mid-artist, try to restore monitored state
            # (only if it originally was monitored and we’re not in dry_run).
            if was_monitored and not dry_run:
                try:
                    if not artist.get("monitored", False):
                        print("  (safety) restoring artist monitored=true…")
                        artist["monitored"] = True
                        artist = lidarr_put(cfg, "artist", artist)
                except Exception as e:
                    print(f"  (warning) failed to restore monitoring state: {e}", flush=True)

        if shutdown_requested:
            print("Shutdown requested — stopping before next artist.")
            break

    print("\nDone.")


if __name__ == "__main__":
    main()
