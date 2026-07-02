#!/usr/bin/env sh
# atoll-proxy one-line installer.
#
# Served by `<server>/install/proxy.sh`; the server templates SERVER_ORIGIN
# in at request time. Typical usage:
#
#   curl -fsSL https://<server>/install/proxy.sh | sh -s -- --api-key <KEY>
#
# Honors optional overrides:
#   --server-ws <ws-url>          override derived server-ws
#   --install-dir <path>          override $HOME/.local/bin
#   ATOLL_PROXY_INSTALL_DIR     same, env-form
#   ATOLL_PROXY_NO_START=1      install + write config, do not auto-start
#
set -eu

# SERVER_ORIGIN is filled in by the server when this script is served via
# /install/proxy.sh (the assignment below is rewritten in-place at request
# time). When the script is fetched as a raw file from disk the value
# stays empty and the caller must pass --server-ws explicitly.
SERVER_ORIGIN=""

api_key=""
server_ws=""
install_dir="${ATOLL_PROXY_INSTALL_DIR:-$HOME/.local/bin}"
auto_start="1"
[ "${ATOLL_PROXY_NO_START:-0}" = "1" ] && auto_start="0"

while [ $# -gt 0 ]; do
  case "$1" in
    --api-key)        api_key="$2"; shift 2 ;;
    --api-key=*)      api_key="${1#*=}"; shift ;;
    --server-ws)      server_ws="$2"; shift 2 ;;
    --server-ws=*)    server_ws="${1#*=}"; shift ;;
    --install-dir)    install_dir="$2"; shift 2 ;;
    --install-dir=*)  install_dir="${1#*=}"; shift ;;
    --no-start)       auto_start="0"; shift ;;
    -h|--help)
      cat <<EOF
atoll-proxy installer

Usage: $0 --api-key <KEY> [--server-ws <ws-url>] [--install-dir <path>] [--no-start]

When fetched from <server>/install/proxy.sh the server bakes its own origin
into the script, so --server-ws can be omitted.
EOF
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$api_key" ]; then
  echo "ERROR: --api-key is required" >&2
  exit 2
fi

if [ -z "$server_ws" ]; then
  if [ -z "$SERVER_ORIGIN" ]; then
    echo "ERROR: --server-ws not provided and script lacks baked-in origin." >&2
    echo "       Fetch from <server>/install/proxy.sh, or pass --server-ws ws://..." >&2
    exit 2
  fi
  # http(s) → ws(s)
  case "$SERVER_ORIGIN" in
    https://*) server_ws="wss://${SERVER_ORIGIN#https://}/devicebus/v2/connect" ;;
    http://*)  server_ws="ws://${SERVER_ORIGIN#http://}/devicebus/v2/connect" ;;
    *) echo "ERROR: server origin not http(s): $SERVER_ORIGIN" >&2; exit 2 ;;
  esac
fi

# When SERVER_ORIGIN is empty (raw-file fetch), the caller must have placed
# the binary themselves. The download branch below is skipped and we just
# write config + start using whatever atoll-proxy is already on $PATH /
# install_dir.
binary_origin="$SERVER_ORIGIN"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64|amd64)   arch="amd64" ;;
  arm64|aarch64)  arch="arm64" ;;
  *) echo "ERROR: unsupported arch: $arch" >&2; exit 1 ;;
esac
case "$os" in
  linux|darwin) ;;
  *) echo "ERROR: unsupported os: $os" >&2; exit 1 ;;
esac

mkdir -p "$install_dir"
binary_path="$install_dir/atoll-proxy"

if [ -n "$binary_origin" ]; then
  binary_url="$binary_origin/install/atoll-proxy_${os}_${arch}"
  echo "downloading $binary_url"
  tmp="$(mktemp)"
  trap 'rm -f "$tmp"' EXIT
  if ! curl -fsSL "$binary_url" -o "$tmp"; then
    echo "ERROR: download failed from $binary_url" >&2
    exit 1
  fi
  chmod +x "$tmp"
  mv "$tmp" "$binary_path"
  echo "✓ installed $binary_path"
else
  if [ ! -x "$binary_path" ]; then
    echo "ERROR: $binary_path not present and no SERVER_ORIGIN to download from." >&2
    exit 1
  fi
  echo "✓ reusing existing $binary_path"
fi

# Write ~/.atoll/proxy/config.json
"$binary_path" install --api-key "$api_key" --server-ws "$server_ws"

if ! command -v "$binary_path" >/dev/null 2>&1; then
  case ":$PATH:" in
    *":$install_dir:"*) ;;
    *)
      echo ""
      echo "NOTE: $install_dir is not in your PATH."
      echo "      Add it to ~/.profile or ~/.zshrc: export PATH=\"$install_dir:\$PATH\""
      ;;
  esac
fi

if [ "$auto_start" = "1" ]; then
  echo ""
  echo "starting atoll-proxy (Ctrl+C to stop; run 'atoll-proxy start' to resume later)"
  echo ""
  exec "$binary_path" start
else
  echo ""
  echo "config written. Start when ready:"
  echo "  $binary_path start"
fi
