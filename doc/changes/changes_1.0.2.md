# Virtual Schema for Databricks 1.0.2, released 2025-??-??

Code name: Fixed vulnerability CVE-2025-53864 in com.google.code.gson:gson:jar:2.10.1:test

## Summary

This release fixes the following vulnerability:

### CVE-2025-53864 (CWE-121) in dependency `com.google.code.gson:gson:jar:2.10.1:test`
github.com/sigstore/sigstore-java (gson) - Stack-based Buffer Overflow [CVE-2025-53864]

A stack-based buffer overflow condition is a condition where the buffer being overwritten is allocated on the stack (i.e., is a local variable or, rarely, a parameter to a function).
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2025-53864?component-type=maven&component-name=com.google.code.gson%2Fgson&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* https://issues.oss-fuzz.com/issues/384541935

## Security

* #53: Fixed vulnerability CVE-2025-53864 in dependency `com.google.code.gson:gson:jar:2.10.1:test`

## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-jdbc:2.7.1` to `2.7.3`
* Updated `com.databricks:databricks-sdk-java:0.42.0` to `0.55.0`
* Updated `com.exasol:exasol-testcontainers:7.1.4` to `7.1.7`
* Updated `com.exasol:hamcrest-resultset-matcher:1.7.0` to `1.7.1`
* Updated `com.exasol:test-db-builder-java:3.6.0` to `3.6.2`
* Updated `com.exasol:virtual-schema-shared-integration-tests:3.0.0` to `3.0.1`
* Updated `org.junit.jupiter:junit-jupiter:5.11.2` to `5.13.3`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:5.0.0` to `5.2.2`
