# databricks-virtual-schema

[![Build Status](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml/badge.svg)](https://github.com/exasol/databricks-virtual-schema/actions/workflows/ci-build.yml)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=security_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=sqale_index)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Adatabricks-virtual-schema&metric=ncloc)](https://sonarcloud.io/dashboard?id=com.exasol%3Adatabricks-virtual-schema)


# Overview

The **Databricks Virtual Schema** provides an abstraction layer that makes an external Databricks database accessible from an Exasol database through regular SQL commands. The contents of the external Databricks database are mapped to virtual tables which look like and can be queried as any regular Exasol table.

If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository](https://github.com/exasol/virtual-schemas).

## Features

* Access a Databricks data source in read only mode from an Exasol database, using a Virtual Schema.

## Table of Contents

### Information for Users

* [Virtual Schemas User Guide](https://docs.exasol.com/database_concepts/virtual_schemas.htm)
* [Databricks Dialect User Guide](doc/user_guide/user_guide.md)
* [Changelog](doc/changes/changelog.md)
* [Dependencies](dependencies.md)

Find all the documentation in the [Virtual Schemas project](https://github.com/exasol/virtual-schemas/tree/master/doc).

### Information for Developers
* [Developer Guide](doc/developer_guide/developer_guide.md)
