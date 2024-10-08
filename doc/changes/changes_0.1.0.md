# Virtual Schema for Databricks 0.1.0, released 2024-09-23

Code name: Initial implementation

## Summary

The **Databricks Virtual Schema** provides an abstraction layer that makes an external Databricks database accessible from an Exasol database through regular SQL commands. The contents of the external Databricks database are mapped to virtual tables which look like and can be queried as any regular Exasol table.

If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository](https://github.com/exasol/virtual-schemas).

**Important:** This project is work in progress. Some features are not yet implemented. See [known limitations](../../README.md#known-limitations) for details.

## Features

* #11: Added query pushdown
* #21: Added support for most important pushdown capabilities
* #10: Added support for `ALTER VIRTUAL SCHEMA <vs> REFRESH`
* #12: Added support for `ALTER VIRTUAL SCHEMA <vs> SET ...`

## Refactoring

* #16: Added shellcheck to CI build

## Documentation

* #22: Added user guide

## Dependency Updates

### Test Dependency Updates

* Added `com.databricks:databricks-jdbc:2.6.40-patch-1`
* Added `com.databricks:databricks-sdk-java:0.31.1`
* Added `com.exasol:exasol-testcontainers:7.1.1`
* Added `com.exasol:hamcrest-resultset-matcher:1.7.0`
* Added `com.exasol:maven-project-version-getter:1.2.0`
* Added `com.exasol:test-db-builder-java:3.5.4`
* Added `com.exasol:virtual-schema-shared-integration-tests:3.0.0`
* Added `org.hamcrest:hamcrest:3.0`
* Added `org.itsallcode:hamcrest-auto-matcher:0.8.1`
* Added `org.junit-pioneer:junit-pioneer:2.2.0`
* Added `org.junit.jupiter:junit-jupiter:5.11.0`
* Added `org.slf4j:slf4j-jdk14:2.0.16`

### Plugin Dependency Updates

* Added `com.exasol:error-code-crawler-maven-plugin:2.0.3`
* Added `com.exasol:project-keeper-maven-plugin:4.3.3`
* Added `io.github.zlika:reproducible-build-maven-plugin:0.16`
* Added `org.apache.maven.plugins:maven-clean-plugin:2.5`
* Added `org.apache.maven.plugins:maven-compiler-plugin:3.13.0`
* Added `org.apache.maven.plugins:maven-dependency-plugin:2.8`
* Added `org.apache.maven.plugins:maven-deploy-plugin:2.7`
* Added `org.apache.maven.plugins:maven-enforcer-plugin:3.5.0`
* Added `org.apache.maven.plugins:maven-failsafe-plugin:3.2.5`
* Added `org.apache.maven.plugins:maven-install-plugin:2.4`
* Added `org.apache.maven.plugins:maven-jar-plugin:2.4`
* Added `org.apache.maven.plugins:maven-resources-plugin:2.6`
* Added `org.apache.maven.plugins:maven-site-plugin:3.3`
* Added `org.apache.maven.plugins:maven-surefire-plugin:3.2.5`
* Added `org.apache.maven.plugins:maven-toolchains-plugin:3.2.0`
* Added `org.basepom.maven:duplicate-finder-maven-plugin:2.0.1`
* Added `org.codehaus.mojo:exec-maven-plugin:3.4.1`
* Added `org.codehaus.mojo:flatten-maven-plugin:1.6.0`
* Added `org.codehaus.mojo:versions-maven-plugin:2.16.2`
* Added `org.jacoco:jacoco-maven-plugin:0.8.12`
* Added `org.sonarsource.scanner.maven:sonar-maven-plugin:4.0.0.4121`
* Added `org.sonatype.ossindex.maven:ossindex-maven-plugin:3.2.0`
