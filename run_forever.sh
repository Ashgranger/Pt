#!/usr/bin/env bash
# Keeps the bot alive for days. Usage: ./run_forever.sh   (loads .env.nvda_patched)
cd "$(dirname "$0")"
set -a; source .env.nvda_patched; set +a
while true; do
  python3 main.py
  code=$?
  echo "$(date -u +%FT%TZ) bot exited code=$code, restarting in 15s" >> restarts.log
  sleep 15
done
