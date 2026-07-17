#!/usr/bin/env python3
"""
Collecteur forward de TWAPs Hyperliquid (via Hypurrscan) + alertes live.

- Polle /twap/{coin} sur une watchlist toutes les N secondes
- Archive chaque TWAP dédupliqué par hash dans {DATA_DIR}/twap_archive.jsonl
  (append-only : aucun événement n'est jamais perdu ni réécrit)
- Met à jour le champ 'ended' quand un TWAP se termine ou est annulé
- Alerte console quand un TWAP dépasse un seuil de taille relative au volume 24h
- Heartbeat toutes les ~30 min pour vérifier que le worker est vivant

Railway : monter un volume sur /data et définir DATA_DIR=/data
(+ PYTHONUNBUFFERED=1 pour des logs immédiats).

Tourne en continu :
    python collect_twaps.py --coins HYPE PURR FARTCOIN kPEPE WIF --interval 120

Après 3-4 semaines, convertit l'archive pour le backtest :
    python collect_twaps.py --export
    python backtest_twap.py --min-adv-pct 0.5
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

DATA_DIR = os.environ.get("DATA_DIR", "data")
ARCHIVE = os.path.join(DATA_DIR, "twap_archive.jsonl")
HYPURR = "https://api.hypurrscan.io"
HL_INFO = "https://api.hyperliquid.xyz/info"
HEADERS = {"User-Agent": "twap-collector/1.0"}


def now_iso():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def load_archive():
    seen = {}
    if os.path.exists(ARCHIVE):
        with open(ARCHIVE) as f:
            for line in f:
                rec = json.loads(line)
                seen[rec["hash"]] = rec
    return seen


def append_archive(rec):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ARCHIVE, "a") as f:
        f.write(json.dumps(rec) + "\n")


def get_ctx():
    """Prix mid + volume 24h de tous les coins (1 requête)."""
    r = requests.post(HL_INFO, json={"type": "metaAndAssetCtxs"}, timeout=20)
    r.raise_for_status()
    meta, ctxs = r.json()
    out = {}
    for asset, ctx in zip(meta["universe"], ctxs):
        out[asset["name"]] = dict(mid=float(ctx.get("midPx") or 0),
                                  vol24=float(ctx.get("dayNtlVlm") or 0))
    return out


def poll_coin(coin, seen, ctx, alert_adv_pct):
    try:
        r = requests.get(f"{HYPURR}/twap/{coin}", headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return 0
        data = r.json()
    except Exception as e:
        print(f"[{now_iso()}] {coin}: erreur poll ({e})", flush=True)
        return 0

    new = 0
    for it in data if isinstance(data, list) else []:
        tw = (it.get("action") or {}).get("twap") or {}
        h = it.get("hash")
        if not tw or not h:
            continue
        prev = seen.get(h)
        rec = dict(hash=h, coin=coin, user=it.get("user"),
                   time=it.get("time"), b=tw.get("b"), s=tw.get("s"),
                   m=tw.get("m"), r=tw.get("r"),
                   ended=it.get("ended"), seen_at=int(time.time() * 1000))
        if prev is None:
            append_archive(rec)
            seen[h] = rec
            new += 1
            c = ctx.get(coin, {})
            ntl = float(tw.get("s") or 0) * c.get("mid", 0)
            adv_pct = 100 * ntl / c["vol24"] if c.get("vol24") else 0
            side = "BUY " if tw.get("b") else "SELL"
            mins = tw.get("m", 0)
            end_at = pd.to_datetime(int(it["time"]), unit="ms", utc=True) \
                + pd.Timedelta(minutes=mins)
            flag = "  <<< ALERTE TAILLE" if adv_pct >= alert_adv_pct else ""
            print(f"[{now_iso()}] NOUVEAU TWAP {side} {coin} "
                  f"~${ntl:,.0f} ({adv_pct:.2f}% du vol 24h) sur {mins}min, "
                  f"fin {end_at:%d/%m %H:%M} UTC{flag}", flush=True)
        elif prev.get("ended") != it.get("ended"):
            append_archive(rec)  # nouvelle ligne = mise à jour d'état
            seen[h] = rec
            status = "ANNULÉ" if it.get("ended") == "error" else "TERMINÉ"
            print(f"[{now_iso()}] TWAP {coin} {h[:10]}... -> {status}",
                  flush=True)
    return new


def export():
    """Archive -> {DATA_DIR}/twaps.parquet au format attendu par
    backtest_twap.py. Garde le dernier état de chaque hash ;
    exclut annulés et encore actifs."""
    seen = load_archive()
    rows = []
    now = pd.Timestamp.now(tz="UTC")
    for rec in seen.values():
        if rec.get("ended") == "error" or not rec.get("m"):
            continue
        t_start = pd.to_datetime(int(rec["time"]), unit="ms", utc=True)
        if isinstance(rec.get("ended"), (int, float)):
            t_end, src = pd.to_datetime(
                int(rec["ended"]), unit="ms", utc=True), "ended"
        else:
            t_end, src = t_start + pd.Timedelta(minutes=float(rec["m"])), \
                "scheduled"
        if t_end >= now:
            continue
        rows.append(dict(coin=rec["coin"], user=rec.get("user"),
                         side=1 if rec.get("b") else -1,
                         size=float(rec.get("s") or 0),
                         minutes=float(rec["m"]), t_start=t_start,
                         t_end=t_end, end_source=src,
                         executed_ntl=0.0, executed_sz=0.0))
    df = pd.DataFrame(rows).sort_values("t_start")
    out = os.path.join(DATA_DIR, "twaps.parquet")
    df.to_parquet(out, index=False)
    print(f"{len(df)} TWAPs exploitables -> {out}")
    if len(df):
        print(df.groupby("coin").size().to_string())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coins", nargs="+",
                    default=["HYPE", "PURR", "FARTCOIN", "kPEPE", "WIF"])
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--alert-adv-pct", type=float, default=0.5)
    ap.add_argument("--export", action="store_true")
    args = ap.parse_args()

    if args.export:
        export()
        return

    seen = load_archive()
    print(f"Collecteur démarré | watchlist: {', '.join(args.coins)} | "
          f"{len(seen)} TWAPs déjà en archive | poll {args.interval}s",
          flush=True)
    ctx_ts = 0
    ctx = {}
    cycle = 0
    while True:
        try:
            if time.time() - ctx_ts > 300:
                ctx = get_ctx()
                ctx_ts = time.time()
            for coin in args.coins:
                poll_coin(coin, seen, ctx, args.alert_adv_pct)
                time.sleep(1)
            cycle += 1
            if cycle % 15 == 0:
                print(f"[{now_iso()}] heartbeat | {len(seen)} TWAPs en archive "
                      f"| cycle {cycle}", flush=True)
        except KeyboardInterrupt:
            print("Arrêt propre. Archive préservée.", flush=True)
            break
        except Exception as e:
            print(f"[{now_iso()}] erreur boucle: {e} — retry dans 30s",
                  flush=True)
            time.sleep(30)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
