# Virtual Schema for Databricks 0.3.0, released 2024-??-??

Code name: Convert names to upper case, fixed vulnerability CVE-2024-47554 in commons-io:commons-io:jar:2.13.0:test

## Summary

This release converts the names of Databricks tables and columns to upper case in Exasol to improve usability of the virtual schema. Quoting table and column names with double quotes `"` is no longer required as Exasol converts all names to upper case by default.

This release fixes the following vulnerability:

### CVE-2024-47554 (CWE-400) in dependency `commons-io:commons-io:jar:2.13.0:test`
Uncontrolled Resource Consumption vulnerability in Apache Commons IO.

The org.apache.commons.io.input.XmlStreamReader class may excessively consume CPU resources when processing maliciously crafted input.

This issue affects Apache Commons IO: from 2.0 before 2.14.0.

Users are recommended to upgrade to version 2.14.0 or later, which fixes the issue.
#### References
* https://ossindex.sonatype.org/vulnerability/CVE-2024-47554?component-type=maven&component-name=commons-io%2Fcommons-io&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2024-47554
* https://lists.apache.org/thread/6ozr91rr9cj5lm0zyhv30bsp317hk5z1

## Security

* #41: Fixed vulnerability CVE-2024-47554 in dependency `commons-io:commons-io:jar:2.13.0:test`

## Features

* #18: Convert table and column names to upper case

## Documentation

* #32: Updated JDBC driver installation instructions in user guide
## Dependency Updates

### Test Dependency Updates

* Updated `com.databricks:databricks-sdk-java:0.32.0` to `0.32.1`
* Updated `org.junit.jupiter:junit-jupiter:5.11.1` to `5.11.2`
