#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PLUGINS_DIR="$ROOT/plugins"

build_plugin() {
    local dir="$1"
    local name

    name="$(basename "$dir")"

    # Skip if not a Cargo project
    [[ -f "$dir/Cargo.toml" ]] || return 0

    echo "Building plugin: $name"

    (
        cd "$dir"
        cargo build --release --target wasm32-wasip2 2>&1
    )

    local wasm_path="$dir/target/wasm32-wasip2/release/${name}.wasm"
    local to_copy_path="$PLUGINS_DIR/${name}.wasm"

    if [[ ! -f "$wasm_path" ]]; then
        echo "ERROR: wasm file not found: $wasm_path"
        exit 1
    fi

    cp "$wasm_path" "$to_copy_path"

    echo "  -> $wasm_path -> copying $to_copy_path"
}

for dir in "$PLUGINS_DIR"/*; do
    [[ -d "$dir" ]] || continue
    build_plugin "$dir"
done

echo "All plugins built."