#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PLUGINS_DIR="$ROOT/plugins"
WIT_DIR="$ROOT/wit"

# Each plugin holds its own copy of the WIT because wit_bindgen::generate!
# resolves `path:` relative to the plugin crate. `$ROOT/wit` is the only source
# of truth — mirror it into every plugin before building so the copies cannot
# drift. A drift is invisible at build time and only surfaces as a type or
# instantiation failure at plugin load, so warn when we actually overwrite
# something (most likely someone edited the copy instead of the root).
sync_wit() {
    local dir="$1"
    local name="$2"

    if [[ -d "$dir/wit" ]] && ! diff -rq "$WIT_DIR" "$dir/wit" >/dev/null 2>&1; then
        echo "  WARNING: $name/wit differed from $ROOT/wit — overwriting from root"
    fi

    mkdir -p "$dir/wit"
    rsync -a --delete "$WIT_DIR/" "$dir/wit/"
}

build_plugin() {
    local dir="$1"
    local name

    name="$(basename "$dir")"

    # Skip if not a Cargo project
    [[ -f "$dir/Cargo.toml" ]] || return 0

    echo "Building plugin: $name"

    sync_wit "$dir" "$name"

    (
        cd "$dir"
        cargo +nightly build --release --target wasm32-wasip2 2>&1
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