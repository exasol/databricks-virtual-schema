# Databricks Virtual Schema Developer Guide

## Initial Setup

### Configure Databricks Credentials for Integration Tests

Create file `test.properties` with the following content, adapting the values for your test environment. Follow the [user guide](../user_guide/user_guide.md#authentication) to get the credentials.

```properties
databricks.token = abcdefg1234567890hijklmopqrstuvwxyz1
databricks.host = https://abc-1234abcd-5678.cloud.databricks.com
databricks.storageRoot = s3://databricks-workspace-stack-123abc-bucket/unity-catalog/1234500000/
databricks.oauth.secret = abcdefg1234567890hijklmopqrstuvwxyz1
databricks.oauth.clientId = 123abc45-def7-89gh-1234-567890abcdef
```

### Install Test and Build Dependencies

```sh
luarocks install --local busted
luarocks install --local ldoc
luarocks install --local --server=https://luarocks.org/dev luaformatter
```

### Install Lua Type Definitions

Run the following script to download / update Lua type definitions for third party libraries. They will be stored in `target/lua-type-definitions/`.

```sh
./tools/fetch-lua-type-definitions.sh
```

### VSCode

1. Install recommended extensions (see [`.vscode/extensions.json`](../../.vscode/extensions.json))
2. Configure `busted` using the [Lua add-on manager](https://luals.github.io/wiki/addons/#addon-manager) to get type hints and documentation when writing Lua unit tests

## Debug Logging for UDF

Class `TestSetup` automatically starts `UdfLogCapturer` listening on a local address. It configures the virtual schema with the correct `DEBUG_ADDRESS` and `LOG_LEVEL` and logs messages from the UDF with prefix `Client #1>`.

## Run Lua Tests

The following script will run Lua tests and print test coverage:

```sh
# Run unit and integration tests
./tools/runtests.sh
# Run only unit tests
./tools/runtests.sh --run=utest
```

## Format Lua Sources

```sh
./tools/format-lua.sh
```

## Lua Type Checking

This project uses [type annotations](https://luals.github.io/wiki/annotations/) of the [Lua Language Server](https://luals.github.io/). You should get type hints and warnings in your IDE.

You can run the type checker on the command line using

```sh
./tools/run-type-check.sh
```
