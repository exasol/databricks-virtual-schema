#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

base_dir="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"
readonly base_dir

readonly src_module_path="$base_dir/src/main/lua"
readonly test_module_path="$base_dir/src/test/lua"

luacheck "$src_module_path" "$test_module_path" --max-line-length 120 --codes --exclude-files src/main/lua/entry.lua

# (W111) setting non-standard global variable adapter_call
luacheck src/main/lua/entry.lua --max-line-length 120 --codes --ignore 111
