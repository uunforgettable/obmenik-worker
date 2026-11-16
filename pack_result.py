"""Упаковывает результат работы над куском. Запуск: python pack_result.py c0001 cards"""
import gzip
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import crawl

chunk_id, kind = sys.argv[1], sys.argv[2]
os.makedirs("_out", exist_ok=True)
if not os.path.exists("senkuro_cards.db"):
    crawl.init_cards_db(sqlite3.connect("senkuro_cards.db"))

for name, out in (("seed_users.db", "result_seed.db.gz"), ("senkuro_cards.db", "result_cards.db.gz")):
    snap = os.path.join("_out", name)
    if os.path.exists(snap):
        os.remove(snap)
    src, dst = sqlite3.connect(name), sqlite3.connect(snap)
    src.backup(dst)                       # согласованный снимок, даже если запись ещё шла
    res = dst.execute("PRAGMA integrity_check").fetchone()[0]
    src.close(); dst.close()
    if res != "ok":
        sys.exit(f"{name} повреждён ({res[:60]}), результат не публикуется")
    with open(snap, "rb") as f_in, gzip.open(out, "wb", 6) as f_out:
        shutil.copyfileobj(f_in, f_out)

s = sqlite3.connect("seed_users.db")
meta = {
    "chunk_id": chunk_id, "kind": kind,
    "helper": os.getenv("GITHUB_REPOSITORY_OWNER", "unknown"),
    "run_id": os.getenv("GITHUB_RUN_ID", ""),
    "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "users_done": s.execute("SELECT COUNT(*) FROM seed_users WHERE status='done'").fetchone()[0],
    "queries_done": s.execute("SELECT COUNT(*) FROM search_queue WHERE status='done'").fetchone()[0],
}
with open("meta.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False)
print("Готово к публикации:", meta)
