# Databricks Virtual Schema Developer Guide

## Initial Setup

### Install Test and Build Dependencies

```sh
luarocks install --local busted
luarocks install --local ldoc
luarocks install --local --server=https://luarocks.org/dev luaformatter
```

### VSCode

1. Install recommended extensions (see [`.vscode/extensions.json`](../../.vscode/extensions.json))
2. Configure `busted` using the [Lua add-on manager](https://luals.github.io/wiki/addons/#addon-manager) to get type hints and documentation when writing Lua unit tests

## Debug Logging for UDF

Class `TestSetup` automatically starts `UdfLogCapturer` listening on a local address. It configures the virtual schema with the correct `DEBUG_ADDRESS` and `LOG_LEVEL` and logs messages from the UDF with prefix `udf>`.

## Format Lua Sources

```sh
./tools/format-lua.sh
```
