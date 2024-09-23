# databricks-virtual-schema

[![Build Status](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml/badge.svg)](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml)

# ⚠️ Work in progress ⚠️

This is the initial release. Some features are not yet implemented. See [known limitations](#known-limitations) for details.

# Overview

The **Databricks Virtual Schema** provides an abstraction layer that makes an external Databricks database accessible from an Exasol database through regular SQL commands. The contents of the external Databricks database are mapped to virtual tables which look like and can be queried as any regular Exasol table.

If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository](https://github.com/exasol/virtual-schemas).

## Features

* Access a Databricks data source in read only mode from an Exasol database, using a Virtual Schema.

## Known Limitations

* Currently, the virtual schema only supports token authentication. M2M Principal authentication (OAuth M2M) will be added later, see [issue #3](https://github.com/exasol/databricks-virtual-schema/issues/3).
* Currently, the virtual schema supports at most 50 tables per virtual schema, see [issue #8](https://github.com/exasol/databricks-virtual-schema/issues/8).
* Currently, the table and column names are case-sensitive, i.e. you need to specify names exactly in upper/lower case as in Databricks, see [issue #18](https://github.com/exasol/databricks-virtual-schema/issues/18).
* Creating a virtual schema fails when a source table uses an unknown or unsupported data type, see [issue #15](https://github.com/exasol/databricks-virtual-schema/issues/15).
* The `TABLE_FILTER` option is not yet supported, see [issue #14](https://github.com/exasol/databricks-virtual-schema/issues/14).

## Table of Contents

### Information for Users

* [Virtual Schemas User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Databricks Dialect User Guide](doc/user_guide/user_guide.md)
* [Changelog](doc/changes/changelog.md)
* [Dependencies](dependencies.md)

Find all the documentation in the [Virtual Schemas project](https://github.com/exasol/virtual-schemas/tree/master/doc).

### Information for Developers

* [Developer Guide](doc/developer_guide/developer_guide.md)
* [Design](doc/developer_guide/design.md)
