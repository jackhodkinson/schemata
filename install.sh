#!/usr/bin/env sh
set -eu

REPO="jackhodkinson/schemata"
BIN_NAME="schemata"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

usage() {
  cat <<'EOF'
Install schemata from GitHub Releases.

Usage:
  curl -fsSL https://raw.githubusercontent.com/jackhodkinson/schemata/main/install.sh | sh

Optional environment variables:
  VERSION=v1.2.3      Install a specific release tag (defaults to latest)
  INSTALL_DIR=/path   Install directory (defaults to ~/.local/bin)
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need_cmd curl
need_cmd tar

if command -v sha256sum >/dev/null 2>&1; then
  checksum_cmd="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  checksum_cmd="shasum -a 256"
elif command -v openssl >/dev/null 2>&1; then
  checksum_cmd="openssl dgst -sha256"
else
  echo "error: need one of sha256sum, shasum, or openssl" >&2
  exit 1
fi

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"

case "$os" in
  linux|darwin) ;;
  *)
    echo "error: unsupported OS: $os" >&2
    exit 1
    ;;
esac

case "$arch" in
  x86_64|amd64) arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)
    echo "error: unsupported architecture: $arch" >&2
    exit 1
    ;;
esac

if [ -n "${VERSION:-}" ]; then
  version="$VERSION"
else
  version="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | sed -n 's/.*"tag_name": "\(v[^"]*\)".*/\1/p' | head -n 1)"
fi

if [ -z "${version:-}" ]; then
  echo "error: failed to resolve release version" >&2
  exit 1
fi

artifact="${BIN_NAME}_${version}_${os}_${arch}.tar.gz"
base_url="https://github.com/$REPO/releases/download/$version"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Installing $BIN_NAME $version for $os/$arch..."

curl -fsSL "$base_url/$artifact" -o "$tmp_dir/$artifact"
curl -fsSL "$base_url/checksums.txt" -o "$tmp_dir/checksums.txt"

expected="$(awk "\$2 == \"$artifact\" { print \$1 }" "$tmp_dir/checksums.txt")"
if [ -z "$expected" ]; then
  echo "error: checksum entry missing for $artifact" >&2
  exit 1
fi

if [ "$checksum_cmd" = "sha256sum" ]; then
  actual="$(sha256sum "$tmp_dir/$artifact" | awk '{print $1}')"
elif [ "$checksum_cmd" = "shasum -a 256" ]; then
  actual="$(shasum -a 256 "$tmp_dir/$artifact" | awk '{print $1}')"
else
  actual="$(openssl dgst -sha256 "$tmp_dir/$artifact" | awk '{print $2}')"
fi

if [ "$actual" != "$expected" ]; then
  echo "error: checksum mismatch for $artifact" >&2
  exit 1
fi

mkdir -p "$tmp_dir/unpack"
tar -xzf "$tmp_dir/$artifact" -C "$tmp_dir/unpack"

binary_path="$tmp_dir/unpack/${BIN_NAME}_${version}_${os}_${arch}/$BIN_NAME"
if [ ! -f "$binary_path" ]; then
  echo "error: release archive missing $BIN_NAME binary" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
cp "$binary_path" "$INSTALL_DIR/$BIN_NAME"
chmod +x "$INSTALL_DIR/$BIN_NAME"

echo "Installed to $INSTALL_DIR/$BIN_NAME"
echo
echo "If needed, add to your PATH:"
echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
echo
echo "Verify installation:"
echo "  $BIN_NAME --version"
