# Virtual Schema for Databricks 0.3.0, released 2024-??-??

Code name: Convert names to upper case, M2M auth

## Summary

This release converts the names of Databricks tables and columns to upper case in Exasol to improve usability of the virtual schema. Quoting table and column names with double quotes `"` is no longer required as Exasol converts all names to upper case by default.

The release also implement OAuth M2M authentication, see the [user guide](../user_guide/user_guide.md#service-principal-oauth-m2m) for details.

## Features

* #18: Convert table and column names to upper case
* #3: Implemented OAuth M2M authentication

## Documentation

* #32: Updated JDBC driver installation instructions in user guide
## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-sdk-java:0.32.0` to `0.32.1`
* Added `commons-io:commons-io:2.17.0`
* Updated `org.junit.jupiter:junit-jupiter:5.11.1` to `5.11.2`
