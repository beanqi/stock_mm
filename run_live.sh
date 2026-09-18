#!/usr/bin/env bash
# 管理 stock_mm 实盘进程（后台运行，日志在 logs/live-<ts>.log，logs/latest.log 指向最新）。
#
#   ./run_live.sh            等同 start
#   ./run_live.sh start      编译并后台启动
#   ./run_live.sh stop       发 SIGINT：程序先撤掉全部挂单再退出（等最多 30 秒）
#   ./run_live.sh restart    stop 后再 start
#   ./run_live.sh status     显示是否在运行
#   ./run_live.sh logs       tail -f 最新日志
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs
PIDFILE=logs/stock_mm.pid
STOP_TIMEOUT=${STOP_TIMEOUT:-30}

running_pid() {
  [ -f "$PIDFILE" ] || return 1
  local pid
  pid=$(cat "$PIDFILE")
  kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

do_start() {
  if pid=$(running_pid); then
    echo "already running (pid $pid)"; return 1
  fi
  cargo build --release
  local ts
  ts=$(date +%Y%m%d-%H%M%S)
  RUST_LOG=${RUST_LOG:-info,stock_mm=info} nohup ./target/release/stock_mm --config config.toml --live > "logs/live-$ts.log" 2>&1 &
  echo $! > "$PIDFILE"
  ln -sf "live-$ts.log" logs/latest.log
  echo "started pid $(cat "$PIDFILE"); tail -f logs/latest.log"
}

do_stop() {
  local pid
  if ! pid=$(running_pid); then
    echo "not running"; rm -f "$PIDFILE"; return 0
  fi
  echo "stopping pid $pid (cancelling orders first)..."
  kill -INT "$pid"
  local i=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$i" -ge "$STOP_TIMEOUT" ]; then
      echo "still alive after ${STOP_TIMEOUT}s; sending SIGKILL (check open orders on Gate manually!)"
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
    sleep 1; i=$((i+1))
  done
  rm -f "$PIDFILE"
  echo "stopped"
}

do_status() {
  if pid=$(running_pid); then
    echo "running (pid $pid), log: logs/$(readlink logs/latest.log 2>/dev/null || echo '?')"
  else
    echo "not running"; return 1
  fi
}

case "${1:-start}" in
  start)   do_start ;;
  stop)    do_stop ;;
  restart) do_stop; do_start ;;
  status)  do_status ;;
  logs)    exec tail -f logs/latest.log ;;
  *) echo "usage: $0 {start|stop|restart|status|logs}"; exit 2 ;;
esac
