#!/usr/bin/env sh
set -eu

version="${COAGENT_PROXY_VERSION:-latest}"
install_dir="${COAGENT_PROXY_INSTALL_DIR:-$HOME/.local/bin}"
base_url="${COAGENT_PROXY_BASE_URL:-https://github.com/wanpengxie/ActOS/releases/download}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac

case "$os" in
  linux|darwin) ;;
  *) echo "unsupported os: $os" >&2; exit 1 ;;
esac

if [ "$version" = "latest" ]; then
  url="$base_url/latest/coagent-proxy_${os}_${arch}"
else
  url="$base_url/$version/coagent-proxy_${os}_${arch}"
fi

mkdir -p "$install_dir"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "downloading $url"
curl -fsSL "$url" -o "$tmp"
chmod +x "$tmp"
mv "$tmp" "$install_dir/coagent-proxy"

echo "installed $install_dir/coagent-proxy"
echo "run: coagent-proxy start --api-key <key> --server-ws <url>"
