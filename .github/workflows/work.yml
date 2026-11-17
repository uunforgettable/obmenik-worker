name: Work chunk

# Помощник: Actions -> Work chunk -> Run workflow. Ничего вводить не нужно:
# кусок, назначенный твоему репозиторию, определяется автоматически.
on:
  workflow_dispatch:
    inputs:
      chunk:
        description: "Номер куска (необязательно; оставь пустым, возьмётся назначенный тебе)"
        required: false
        default: ""

concurrency:
  group: work
  cancel-in-progress: false

permissions:
  contents: write     # чтобы выложить результат релизом в этом репозитории

env:
  HUB_REPO: uunforgettable/obmenik-worker    # публичный репозиторий, где лежат куски (задаёт владелец проекта)

jobs:
  work:
    runs-on: ubuntu-latest
    timeout-minutes: 350
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - run: pip install httpx

      - name: Pick and download chunk
        env:
          GH_TOKEN: ${{ github.token }}
          CHUNK_INPUT: ${{ inputs.chunk }}
          ME: ${{ github.repository }}
        run: |
          gh release download queue -R "$HUB_REPO" --pattern index.json --clobber
          python - <<'PY'
          import json, os, re, sys
          idx = json.load(open("index.json", encoding="utf-8"))
          if idx.get("paused"):
              sys.exit("Работа сейчас приостановлена владельцем проекта. Попробуй позже.")
          me = os.environ["ME"].lower()
          want = os.environ.get("CHUNK_INPUT", "").strip()
          chunks = idx["chunks"]
          if want:
              if not re.fullmatch(r"[cs]\d{4}", want):
                  sys.exit("Номер куска должен выглядеть как c0001 или s0001")
              mine = [c for c in chunks if c["id"] == want]
          else:
              mine = [c for c in chunks if (c.get("assigned_to") or "").lower() == me]
          if not mine:
              sys.exit("Для тебя пока нет куска. Подожди, пока владелец проекта выдаст новый (обычно в течение пары часов), и попробуй позже.")
          chunk = mine[0]["id"]
          p = idx["params"]
          with open(os.environ["GITHUB_ENV"], "a") as f:
              f.write(f"CHUNK={chunk}\nDELAY={p['delay']}\nWORKERS={p['workers']}\nMAX_DEPTH={p['max_depth']}\nHOURS={p['hours']}\n")
          print("Твой кусок:", chunk)
          PY
          source "$GITHUB_ENV" 2>/dev/null || true
          CHUNK=$(grep '^CHUNK=' "$GITHUB_ENV" | cut -d= -f2)
          # уже сдавал этот кусок? тогда ждём новый
          if gh release list --json tagName -q '.[].tagName' | grep -q "^result-${CHUNK}-"; then
            echo "::error::Кусок ${CHUNK} уже сдан. Подожди, пока владелец проекта выдаст новый."; exit 1
          fi
          gh release download queue -R "$HUB_REPO" --pattern "chunk_${CHUNK}.json.gz" --clobber

      - name: Prepare
        run: python prepare_chunk.py "chunk_${CHUNK}.json.gz"

      - name: Crawl
        env:
          PYTHONUNBUFFERED: "1"
        run: |
          KIND=$(cat kind.txt)
          EXTRA=""
          if [ "$KIND" = "search" ]; then EXTRA="--discover --max-depth $MAX_DEPTH"; fi
          echo "Тип куска: $KIND. Пауза $DELAY, потоков $WORKERS, максимум $HOURS ч."
          set +e
          timeout --signal=INT "${HOURS}h" python crawl.py --limit 1000000 --delay "$DELAY" --workers "$WORKERS" $EXTRA
          code=$?
          echo "Код завершения: $code"
          if [ "$code" -ne 0 ] && [ "$code" -ne 124 ]; then exit "$code"; fi

      - name: Pack and publish result
        if: always()
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          pkill -INT -f "python crawl.py" || true
          sleep 5
          pkill -9 -f "python crawl.py" || true
          python pack_result.py "$CHUNK" "$(cat kind.txt)"
          gh release create "result-${CHUNK}-${GITHUB_RUN_ID}" result_seed.db.gz result_cards.db.gz meta.json \
            --title "Результат ${CHUNK}" --notes "Результат работы над куском ${CHUNK}. Забирается автоматически."
