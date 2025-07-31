# Virtual Schema for Databricks 1.0.2, released 2025-07-31

Code name: Fixed vulnerabilities in test dependencies

## Summary

This release updates dependencies to fix CVE-2025-53864 and CVE-2025-48924 in transitive test dependencies
`com.google.code.gson:gson:jar:2.10.1:test` and `org.apache.commons:commons-lang3:jar:3.17.0:test` respectively.

We also added an exception for the OSSIndex for CVE-2024-55551, which is a false positive in Exasol's JDBC driver.
This issue has been fixed quite a while back now, but the OSSIndex unfortunately does not contain the fix version of 24.2.1 (2024-12-10) set.

## Security

* #49: Fix CVE-2024-55551 in `com.exasol:exasol-jdbc:jar:25.2.2:test`
* #51: Fix CVE-2025-48924 in `org.apache.commons:commons-lang3:jar:3.16.0:test`
* #53: Fix CVE-2025-53864 in `com.google.code.gson:gson:jar:2.10.1:test`

## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-jdbc:2.7.1` to `2.7.3`
* Updated `com.databricks:databricks-sdk-java:0.42.0` to `0.56.0`
* Updated `com.exasol:exasol-testcontainers:7.1.4` to `7.1.7`
* Updated `com.exasol:hamcrest-resultset-matcher:1.7.0` to `1.7.1`
* Updated `com.exasol:test-db-builder-java:3.6.0` to `3.6.2`
* Updated `com.exasol:virtual-schema-shared-integration-tests:3.0.0` to `3.0.1`
* Added `org.junit.jupiter:junit-jupiter-params:5.13.4`
* Removed `org.junit.jupiter:junit-jupiter:5.11.2`
* Added `org.slf4j:jcl-over-slf4j:2.0.17`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:2.0.3` to `2.0.4`
* Updated `com.exasol:project-keeper-maven-plugin:5.0.0` to `5.2.3`
* Added `io.github.git-commit-id:git-commit-id-maven-plugin:9.0.1`
* Removed `io.github.zlika:reproducible-build-maven-plugin:0.17`
* Added `org.apache.maven.plugins:maven-artifact-plugin:3.6.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.5.2` to `3.5.3`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.5.2` to `3.5.3`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.12` to `0.8.13`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:5.0.0.4389` to `5.1.0.4751`
