# Databricks Virtual Schema Developer Guide

## IDE Setup

### VSCode

1. Install recommended extensions (see [`.vscode/extensions.json`](../../.vscode/extensions.json))
2. Configure `busted` using the [Lua add-on manager](https://luals.github.io/wiki/addons/#addon-manager) to get type hints and documentation when writing Lua unit tests

## Enable Debug Logging for UDF

To get logs from the VS UDF start `nc` and get the local IP address as described in the [documentation](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/logging.htm):

Start the listener with

```sh
nc -lkp 3000
# or 
ncat --listen --verbose  --keep-open 3000
```

Then run integration tests with system properties

```
-Dcom.exasol.virtualschema.debug.host=$IP_ADDRESS
-Dcom.exasol.virtualschema.debug.port=3000
-Dcom.exasol.virtualschema.debug.level=TRACE
```

When using VS Code you can specify the properties in [`.vscode/settings.json`](../../.vscode/settings.json):

```json
{
    "java.test.config": {
        "vmArgs": [
            "-Djava.util.logging.config.file=src/test/resources/logging.properties",
            "-Dcom.exasol.virtualschema.debug.host=$IP_ADDRESS",
            "-Dcom.exasol.virtualschema.debug.port=3000",
            "-Dcom.exasol.virtualschema.debug.level=TRACE",
        ]
    }
}
```
