# Software Design

## Architecture

See the [Exasol documentation](https://docs.exasol.com/db/latest/database_concepts/virtual_schemas.htm) for details about virtual schemas.

In short, the virtual schema adapter implemented in this repository is responsible for
* Mapping tables, columns and data types from Databricks to Exasol
* Converting SQL queries to the Databricks dialect ("pushdown") and create an `IMPORT FROM JDBC` statement.

After the adapter created the pushdown query, the Exasol loader will start an ETL job that executes the pushdown query via the Databricks JDBC driver to import the data.

### Databricks JDBC Driver

In order to use a JDBC driver in an `IMPORT FROM JDBC` statement, users need to upload and configure the JDBC driver as described in the [Exasol documentation](https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/add_jdbc_driver.htm) using file `/buckets/bfsdefault/default/drivers/jdbc/settings.cfg`. Configuration option `NOSECURITY` allows to enable or disable the Java security manager.

However when using the Databricks JDBC Driver we need to disable the security manager by setting `NOSECURITY=YES` in `settings.cfg`. Else the execution of `IMPORT FROM JDBC` statements will hang forever and the ETL JDBC log file (e.g. `/exa/logs/db/DB1/20240902_174802_EtlJdbc_13_-1.0`) will contain lines like these repeated forever:

```
2024-09-02 16:48:12.796 debu: poll finished - block=null
2024-09-02 16:48:12.797 debu: trying to poll block
2024-09-02 16:48:22.798 debu: poll finished - block=null
2024-09-02 16:48:22.798 debu: trying to poll block
2024-09-02 16:48:32.806 debu: poll finished - block=null
```

### Databricks `CONNECTION`

The ETL job reads information required to connect to Databricks from a [`CONNECTION` definition](https://docs.exasol.com/db/latest/sql/create_connection.htm).

#### Specifying Username and Password

The [documentation for Databricks JDBC driver](https://docs.databricks.com/en/_extras/documents/Databricks-JDBC-Driver-Install-and-Configuration-Guide.pdf) recommends specifying the token in the JDBC URL like this:

```
jdbc:databricks://node1.example.com:443;AuthMech=3;UID=token;PWD=<databricks-token-content>
```

So all credentials are contained in the URL and specifying username and password is not necessary when connecting via the Databricks JDBC driver.

However the Exasol ETL Job additionally passes properties `user` and `password` to the JDBC driver. When they are not specified in the `CONNECTION` it passes empty strings. Databricks JDBC driver can't accept these empty values and connection fails with an exception:

```
java.sql.SQLException: [Databricks][JDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: HTTP Response code: 401, Error message: Unknown.
...
        at com.databricks.client.jdbc.common.AbstractDriver.connect(Unknown Source)
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:677)
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:189)
Caused by: com.databricks.client.support.exceptions.ErrorException: [Databricks][JDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: HTTP Response code: 401, Error message: Unknown.
        ... 20 more
Caused by: com.databricks.client.jdbc42.internal.apache.thrift.transport.TTransportException: HTTP Response code: 401, Error message: Unknown
...
```

The workaround is to specify username `token` and the actual token as password in the `CONNECTION` definition. This deviates from the Databricks JDBC driver documentation but it works.

An important advantage of this approach is that the JDBC URL which is often contained in logs or error messages does not contain secret credentials.

## Design Decisions

### Implementation Language

We decided to implement the virtual schema adapter in Lua for the following reasons:
* `+` Low latency
* `+` Databricks provides a REST API that can be accessed via HTTPS also from Lua, so the adapter does not need to use JDBC
* `+` Reusable base library `virtual-schema-common-lua`
* `-` Higher implementation effort than with Java

Considered alternatives:
* Java
  * `+` Databricks provides a JDBC driver which allows using existing base library `virtual-schema-common-jdbc`. This implements most of the business logic, only a thin customization layer would be required.
  * `-` High latency due to JVM startup

### Type Annotations

#### Should we use Type Annotations at all?

Lua is a dynamic language. Annotating Lua code with types has the following advantages:
* It improves developer experience, IDEs can provide type hints
* Running a type checker can help avoid runtime errors caused by incompatible types

Considered alternatives:
* Not using type annotations results in bad developer experience and increases the risk of runtime errors.

#### Use Comments as Type Annotations

Using special comments for annotating types has the following advantages:
* `+` Non-invasive, requires no additional build step
* `+` Modules can be published to LuaRocks without a problem. We could migrate all Lua projects (e.g. `virtual-schema-common-lua`) to use type annotations to allow cross-project type checking.
* `-` Comments are harder to read and write than "native" type annotations

Considered alternatives:

* Using a transpiler from a typed language to Lua (e.g. [TypeScriptToLua](https://github.com/TypeScriptToLua/TypeScriptToLua))
  * `+` Provides excellent IDE support (e.g. TypeScript)
  * `-` Increases build time complexity
  * `-` May cause incompatibilities due to different runtime behaviour, e.g. between [TypeScript and Lua](https://typescripttolua.github.io/docs/caveats)
  * `-` Type information is lost when publishing transpiled Lua code to LuaRocks
* Using a typed Lua variant (e.g. [Teal](https://github.com/teal-language/tl))
  * `+` Provides IDE support
  * `-` Requires an additional build step.
  * `-` It's unclear if publishing a Teal-based module to LuaRocks is possible.

#### LuaLS Annotations

We decided to use [Lua Language Server](https://luals.github.io/) (LuaLS) type annotations:
* `+` Provides plugins for VSCode, IntelliJ and Neovim
* `+` Allows running the type checker standalone in the CI build using flag `--check`
* `+` Generates documentation as JSON or Markdown using flag `--doc`
* `-` Standalone mode is not CLI friendly (see [LuaLS issue #2830](https://github.com/LuaLS/lua-language-server/issues/2830)), but there are workarounds

Considered alternatives:
* [EmmyLua](https://github.com/EmmyLua) provides similar but incompatible type annotations
  * `+` Provides plugins for IntelliJ and VSCode
  * `-` Community seems to be less active than LuaLS, repository has less GitHub stars and release
  - `-` Doesn't seem to support type checking in standalone mode during CI-build
  * `+` Currently used in many other Lua projects, e.g. [virtual-schema-common-lua](https://github.com/exasol/virtual-schema-common-lua)
  + `+` A third party tool [lemmy-help](https://github.com/numToStr/lemmy-help) allows generating documentation but is inactive since 2022.
* [LuaDoc](https://keplerproject.github.io/luadoc/) generates documentation based on annotations
  * `-` Annotations are incompatible with LuaLS and EmmyLua
  * `-` No type checker
  * `+` Currently used in project [exasol-driver-lua](https://github.com/exasol/exasol-driver-lua)

### Lua HTTP Client

The VS needs to access the Databricks REST API via HTTPS. We decided to use the `request()` function from the [`socket.http`](https://w3.impa.br/~diego/software/luasocket/http.html) module:
* `+` Available to Exasol UDFs, recommended by [Exasol documentation](https://docs.exasol.com/db/latest/database_concepts/udf_scripts/lua.htm#Auxiliarylibraries).
* `+` Allows using a custom socket factory with the `create` parameter. This allows customizing the TLS configuration if required.
* `-` Does not allow using a custom TLS certificate store, uses the hard coded store included in Exasol.
  * This should not be a problem, we expect that the Databricks API uses valid TLS certificates.

Considered alternatives: None.

### Language for Integration Tests

We write integration tests for the VS in Java:
* `+` Large set of libraries that simplify starting a local Exasol DB (`exasol-testcontainers`), managing DB objects (`test-db-builder-java`) and asserting table content (`hamcrest-resultset-matcher`)
* `+` `exec-maven-plugin` allows integrating the Lua build into the Maven build
* `+` Using Maven allows generating build scripts etc. with `project-keeper`
* `-` Mixing of two languages in a single project

### Convert Table and Column Names to Upper Case

VSDAB converts Databricks table and columns to upper case.

Rationale: Exasol converts unquoted identifiers to upper case. If a table or column contains lower case characters, users need to specify the exact case and quote the identifier in double quotes `"`. This is inconvenient and a potential source of errors.

Converting all names to upper case introduces the risk of conflicts, e.g. when a Databricks table contains columns with the same name but different case, e.g. `COL` and `col`. However this is not a problem, because creating such tables in Databricks is not possible and fails with the following error:

```
Failed to write to object: 'CREATE TABLE `vs-test-cat-1727789463981`.`schema-1727789465842`.`tab1` (`col` VARCHAR(5), `COL` INT)'. Cause: '[Databricks][JDBCDriver](500051) ERROR processing query/statement. Error Code: 0, SQL state: 42711, Query: CREATE TAB***, Error message from Server: org.apache.hive.service.cli.HiveSQLException: Error running query: [COLUMN_ALREADY_EXISTS] org.apache.spark.sql.AnalysisException: [COLUMN_ALREADY_EXISTS] The column `col` already exists. Choose another name or rename the existing column. SQLSTATE: 42711
```

Databricks tables are all in lower case, so tables names cannot create conflicts either.

### Integration Tests

#### Using Dynamic Junit Jupiter Tests

We use [dynamic tests](https://junit.org/junit5/docs/current/user-guide/#writing-tests-dynamic-tests) using `@TestFactory` for verifying that a virtual schema returns the expected data types and maps values as expected.

* `-` More complicated, higher initial effort
* `+` Efficient: reduce time for setup, reuse test data for multiple test cases
* `+` Readable tests cases, technical setup hidden in test framework
* `+` Flexible for future changes
* `+` Test failures directly show column type and mis-matched value

Considered alternatives:

* `@ParameterizedTest`
  * `-` Less flexible with test setup and teardown
  * `-` Would require `@Nested` test classes or only a single `@Test` per class
  * `-` Test failures harder to read, need to read assertion stack traces

#### Databricks Service Principal

In order to verify that M2M OAuth authentication works, integration tests need a Databricks Service Principal with OAuth Client ID and Client Secret. The best way would be that tests automatically create a new service principal incl. OAuth credentials to avoid manual effort for creating and configuring it.

Databricks manages Service Credentials on account level, not on Workspace level. That's why we can't use the existing Workspace-level user token already configured for tests to create a new Service Principal.

#### Service Principal UUID

Integration tests create a new Databricks Catalog for each test class. The Service Principal used for accessing Databricks via M2M OAuth needs read access to this catalog. We could give the Service Principal Administrator access in Databricks, so that it automatically can read all catalogs. However we want to use minimal permissions for the Service Principal.

That's why integration tests need to grant the Service Principal read access to the newly created catalog and all its schemas and tables. We can do this with a `GRANT` statement in Databricks, but this requires configuring the Service Credentials UUID in `test.properties`.

Note: A better option would be to create a temporary service principal in the tests. This is not possible because Databricks manages service principals on Account level, not on Workspace level. It seems to be not possible to create long-term credentials on Account level. The only option are one-time codes sent via Email, which is not an option for automated tests.
