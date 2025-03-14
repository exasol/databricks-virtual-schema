# Virtual Schema for Databricks 1.0.1, released 2025-03-14

Code name: Fix CVE-2024-49194

## Summary

This release fixes CVE-2024-49194 by updating Databricks JDBC driver that was used in the integration tests.

## Security

* #48 Fix CVE-2024-49194 in `com.databricks:databricks-jdbc:2.6.40-patch-1`  

## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-jdbc:2.6.40-patch-1` to `2.7.1`
* Updated `com.databricks:databricks-sdk-java:0.32.2` to `0.42.0`
* Updated `com.exasol:exasol-testcontainers:7.1.1` to `7.1.4`
* Updated `com.exasol:maven-project-version-getter:1.2.0` to `1.2.1`
* Removed `commons-io:commons-io:2.17.0`
* Updated `org.itsallcode:hamcrest-auto-matcher:0.8.1` to `0.8.2`
* Updated `org.slf4j:slf4j-jdk14:2.0.16` to `2.0.17`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:4.3.3` to `5.0.0`
* Added `com.exasol:quality-summarizer-maven-plugin:0.2.0`
* Updated `io.github.zlika:reproducible-build-maven-plugin:0.16` to `0.17`
* Updated `org.apache.maven.plugins:maven-clean-plugin:3.2.0` to `3.4.1`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.13.0` to `3.14.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.2.5` to `3.5.2`
* Updated `org.apache.maven.plugins:maven-install-plugin:3.1.2` to `3.1.4`
* Updated `org.apache.maven.plugins:maven-site-plugin:3.12.1` to `3.21.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.2.5` to `3.5.2`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.6.0` to `1.7.0`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.16.2` to `2.18.0`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:4.0.0.4121` to `5.0.0.4389`
