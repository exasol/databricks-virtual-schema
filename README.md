# databricks-virtual-schema

[![Build Status](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml/badge.svg)](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml)

# Overview

The **Databricks Virtual Schema** (VSDAB) provides an abstraction layer that makes an external Databricks database accessible from an Exasol database through regular SQL commands. The contents of the external Databricks database are mapped to virtual tables which look like and can be queried as any regular Exasol table.

If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository](https://github.com/exasol/virtual-schemas).

## Features

* Access a Databricks data source in read only mode from an Exasol database, using a Virtual Schema.

## Known Limitations

This section lists known issues and limitations that will be fixed in a later version. If you need one of the features, please add a comment to the GitHub issue to help us prioritize it.

### Security

* VSDAB does not verify the TLS certificate of the Databricks server, see [issue #4](https://github.com/exasol/databricks-virtual-schema/issues/4).
* When using M2M OAuth authentication, the Databricks JDBC driver requires entering credentials in the JDBC URL. This could leak credentials via log and error messages. See [issue #43](https://github.com/exasol/databricks-virtual-schema/issues/43). As a workaround you can use PAT token authentication, see [user guide](doc/user_guide/user_guide.md#personal-access-token-pat) for details.

### Features

* Currently, VSDAB supports at most 50 tables per virtual schema. Creating a virtual schema with more tables will fail, see [issue #8](https://github.com/exasol/databricks-virtual-schema/issues/8).
* VSDAB does not yet support the `TABLE_FILTER`, see [issue #14](https://github.com/exasol/databricks-virtual-schema/issues/14).
* VSDAB does not support Databricks column type `BINARY`. Creating a virtual schema with this column type will fail. See [issue #34](https://github.com/exasol/databricks-virtual-schema/issues/34).

### Usability

* Creating a virtual schema for a missing Databricks catalog or schema will fail with a technical 404 error message. See [issue #9](https://github.com/exasol/databricks-virtual-schema/issues/9).

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
