#!/bin/zsh
# Start stock_mm in live mode in the background; logs go to logs/live-<ts>.log.
# Stop with:  kill -INT $(cat logs/stock_mm.pid)   (cancels all orders, then exits)
set -e
cd "$(dirname "$0")"
mkdir -p logs
if [ -f logs/stock_mm.pid ] && kill -0 "$(cat logs/stock_mm.pid)" 2>/dev/null; then
  echo "already running (pid $(cat logs/stock_mm.pid))"; exit 1
fi
cargo build --release
TS=$(date +%Y%m%d-%H%M%S)
RUST_LOG=${RUST_LOG:-info,stock_mm=info} nohup ./target/release/stock_mm --config config.toml --live > "logs/live-$TS.log" 2>&1 &
echo $! > logs/stock_mm.pid
ln -sf "live-$TS.log" logs/latest.log
echo "started pid $(cat logs/stock_mm.pid); tail -f logs/latest.log"
