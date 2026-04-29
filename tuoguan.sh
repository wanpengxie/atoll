#!/bin/bash

SERVER_URL="https://lightcone-production.up.railway.app"
API_KEY="sk_machine_ba7743391777ef24743375243efbdd213a0443200a0fb60954de60596727bc8b"

PIDFILE="/home/ubuntu/lightcone/.tuoguan.pid"
LOGFILE="/home/ubuntu/lightcone/tuoguan.log"

start() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    echo "托管服务已在运行 (pid=$(cat $PIDFILE))"
    return
  fi
  # 用 setsid 让进程独立成组，方便后续用 kill -- -PGID 杀掉整个进程树
  setsid nohup npx --yes @lightcone-ai/daemon@latest \
    --server-url "$SERVER_URL" \
    --api-key "$API_KEY" \
    >> "$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"
  echo "托管服务已启动 (pid=$!), 日志: $LOGFILE"
}

stop() {
  if [ ! -f "$PIDFILE" ]; then
    echo "托管服务未运行"
    return
  fi
  PID=$(cat "$PIDFILE")
  if kill -0 "$PID" 2>/dev/null; then
    # 杀掉整个进程组（npx + 它启动的 node 子进程）
    PGID=$(ps -o pgid= -p "$PID" 2>/dev/null | tr -d ' ')
    if [ -n "$PGID" ] && [ "$PGID" != "0" ]; then
      kill -- "-$PGID" 2>/dev/null
    else
      kill "$PID"
    fi
    # 等待进程真正退出，最多 5 秒
    for i in $(seq 1 10); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.5
    done
    echo "托管服务已停止 (pid=$PID)"
  else
    echo "进程不存在，清理 pid 文件"
  fi
  rm -f "$PIDFILE"
  # 兜底：清理所有漏网的同名孤儿进程
  pkill -f "lightcone-daemon" 2>/dev/null || true
}

status() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    echo "托管服务运行中 (pid=$(cat $PIDFILE))"
  else
    echo "托管服务未运行"
  fi
}

case "$1" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; start ;;
  status)  status ;;
  log)     tail -f "$LOGFILE" ;;
  *)
    echo "用法: $0 {start|stop|restart|status|log}"
    exit 1
    ;;
esac
