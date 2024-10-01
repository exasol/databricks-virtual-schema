# Virtual Schema for Databricks 0.2.0, released 2024-??-??

Code name: Map unsupported data types to VARCHAR

## Summary

This release maps unsupported Databricks data types `BINARY`, `ARRAY`, `MAP`, `STRUCT` and `VARIANT` to Exasol type `VARCHAR`. Creating a virtual schema no longer fails when the source schema contains columns of these types. Please note that type `BINARY` is still not supported and creating a virtual schema with a `BINARY` column fails, see [issue #34](https://github.com/exasol/databricks-virtual-schema/issues/34) for details.

The release also stores the original Databricks metadata for each table and column as JSON format in field `databricks_metadata` of the adapter notes for table and column. This helps with debugging the virtual schema.

## Features

* #15: Mapped unsupported data types to `VARCHAR`
* #33: Store Databricks metadata in adapter notes

## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-sdk-java:0.31.1` to `0.32.0`
* Updated `com.exasol:test-db-builder-java:3.5.4` to `3.6.0`
* Updated `org.junit.jupiter:junit-jupiter:5.11.0` to `5.11.1`
