"""Готовит локальные базы из выданного куска. Запуск: python prepare_chunk.py chunk_c0001.json.gz"""
import gzip
import json
import os
import re
import sqlite3
import sys

chunk_path = sys.argv[1]
with gzip.open(chunk_path, "rt", encoding="utf-8") as f:
    chunk = json.load(f)

kind = chunk.get("kind")
items = chunk.get("items", [])
if kind not in ("cards", "search") or not isinstance(items, list):
    sys.exit("Неверный формат куска")

for name in ("seed_users.db", "senkuro_cards.db"):
    if os.path.exists(name):
        os.remove(name)

db = sqlite3.connect("seed_users.db")
db.execute("""CREATE TABLE seed_users (number TEXT PRIMARY KEY, user_id TEXT NOT NULL, name TEXT,
              status TEXT NOT NULL DEFAULT 'pending', checked_at TEXT)""")
db.execute("""CREATE TABLE search_queue (query TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending',
              found INTEGER, new_users INTEGER, checked_at TEXT)""")
if kind == "cards":
    ok = [(n, u, n) for n, u in items if re.fullmatch(r"\d{1,25}", str(n)) and re.fullmatch(r"[A-Za-z0-9_-]{8,64}", str(u))]
    db.executemany("INSERT OR IGNORE INTO seed_users (number, user_id, name) VALUES (?,?,?)", ok)
else:
    ok = [(q,) for q in items if isinstance(q, str) and 1 <= len(q) <= 6]
    db.executemany("INSERT OR IGNORE INTO search_queue (query) VALUES (?)", ok)
db.commit()
db.close()
with open("kind.txt", "w") as f:
    f.write(kind)
print(f"Кусок {chunk['chunk_id']}: тип {kind}, элементов {len(ok)} из {len(items)}")
