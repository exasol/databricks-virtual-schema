# Databricks Virtual Schema User Guide

Databricks Virtual Schema for Lua (short "VSDAB") is an implementation of a [Virtual Schema](https://docs.exasol.com/db/latest/database_concepts/virtual_schemas.htm).

With VSDAB you can make a read-only connection from a schema in a Databricks database to a so-called "Virtual Schema". A Virtual Schema is a projection of the data in the source schema. It looks and feels like a real schema with the main difference being that you can only read data and not write it.

## Introduction

Each Virtual Schema needs a data source. In the case of Databricks Virtual Schema for Lua, this source is a database schema in a Databricks database. We call that the "origin schema".

Conceptually Virtual Schemas are very similar to database views. They have an owner (typically the one who creates them) and share that owners access permissions. This means that for a Virtual Schema to be useful, the owner must have the permissions to view the source.

Users of the Virtual Schema must have permissions to view the Virtual Schema itself, but they don't need permissions to view the source.

### Virtual Schema Adapter

Each Virtual Schema requires an Adapter. Think of this as a plug-in for Exasol that defines how to access data from a specific source.

Check the section ["Installation"](#installation) for details on how to install the VSDAB adapter. 

### Query Rewriting and Push-Down

The main function of a Virtual Schema is to take a query and turn it into a different one that reads from the data source. The input query &mdash; that means the query users of a Virtual Schema run &mdash; is always a `SELECT` statement.

If your VSDAB uses local access the output query will also be a `SELECT` statement &mdash; after all the data is on the same database.

For VSDAB the output query is an `IMPORT` statement using a JDBC connection to Databricks, thus allowing to get data via a network connection.

The output query is also called "push-down query", since it is pushed down to the data source. See section ["Examining the Push-down query"](#examining-the-push-down-query)

## Installation

What you will need before you begin:

1. Exasol Version 8.31.0 or later
2. A database schema where you can install the adapter script
3. The database privilege to install the script
4. A copy of the adapter script from the [release page](https://github.com/exasol/databricks-virtual-schema/releases) (check for latest release)

   `databricks-virtual-schema-dist-<version>.lua`

Make sure you pick the file with `-dist-` in the name, because that is the installation package that contains everything you need.

5. Connection details for your Databricks database consisting of the JDBC URL and credentials.

### Install the Databricks JDBC Driver

1. Download the latest [Databricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-archive).
2. Unpack the downloaded ZIP file.
2. Upload file `DatabricksJDBC42.jar` to BucketFS under path `default/drivers/jdbc/`, see the [BucketFS documentation](https://docs.exasol.com/db/latest/administration/on-premise/bucketfs/accessfiles.htm) for details.

### Register the JDBC driver for ExaLoader

In order to enable the ExaLoader to fetch data from Databricks you must register the driver for ExaLoader as described in the [Installation procedure for JDBC drivers](https://github.com/exasol/docker-db/#installing-custom-jdbc-drivers).

To do that you need to create file `settings.cfg` and upload it to `default/drivers/jdbc/` in BucketFS:

```properties
DRIVERNAME=DATABRICKS
JAR=DatabricksJDBC42.jar
DRIVERMAIN=com.databricks.client.jdbc.Driver
PREFIX=jdbc:databricks:
NOSECURITY=YES
FETCHSIZE=100000
INSERTSIZE=-1

```

**Important:** Make sure that the file contains a trailing empty line. JDBC driver registration won't work if it is missing.

### Creating a Schema to Hold the Adapter Script

For the purpose of the User Guide we will assume that you install the adapter in a schema called `VSDAB_SCHEMA`.

If you are not the admin the database, please ask an administrator to create that schema for you and grant you write permissions.

```sql
CREATE SCHEMA VSDAB_SCHEMA;
```

### Creating Virtual Schema Adapter Script

Now you need to install the adapter script (i.e. the plug-in that drives the Virtual Schema):

```sql
CREATE OR REPLACE LUA ADAPTER SCRIPT VSDAB_SCHEMA.VSDAB_ADAPTER AS
    table.insert(package.searchers,
        function (module_name)
            local loader = package.preload[module_name]
            if(loader == nil) then
                error("Module " .. module_name .. " not found in package.preload.")
            else
                return loader
            end
        end
    )
    
    <copy the whole content of databricks-virtual-schema-dist-<version>.lua here>
/
;
```

The first fixed part is a module loading preamble that is required with Exasol version 8.

### Create a Named Connection

Create a named connection:

```sql
CREATE OR REPLACE CONNECTION DATABRICKS_JDBC_CONNECTION
TO 'jdbc:databricks://$WORKSPACE_HOST_NAME:$PORT/default;transportMode=http;ssl=1;AuthMech=3;httpPath=$HTTP_PATH;'
USER '$USERNAME'
IDENTIFIED BY '$PASSWORD';
```

#### JDBC URL

Fill in the following placeholders in the JDBC URL:
* `$WORKSPACE_HOST_NAME`: Hostname of your Databricks workspace, e.g. `abc-1234abcd-5678.cloud.databricks.com`
* `$PORT`: Port of your Databricks workspace, usually `443`
* `$HTTP_PATH`: Partial URL corresponding to the Spark server, e.g. `/sql/1.0/warehouses/abc123def456ghi7`

You can find the JDBC URL by logging in to your Databricks workspace:

1. Click on entry "SQL Warehouses" in the left menu bar
2. Click on tab "SQL Warehouses"
3. Click on the entry for your SQL Warehouse
4. Click on tab "Connection details"
5. Select JDBC URL for version "2.6.5 or later"

Example JDBC URL:

```
jdbc:databricks://abc-1234abcd-5678.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/abc123def456ghi7;
```

#### Authentication

Enter credentials in `USER` and `IDENTIFIED BY` fields of the connection. You have two options for authentication.

##### Personal Access Token (PAT)

Create a "personal access token" in your workspace as described in the [Databricks documentation](https://docs.databricks.com/en/dev-tools/auth/pat.html).

Once you have generated your token, enter it in the connection:

```sql
CREATE OR REPLACE CONNECTION DATABRICKS_JDBC_CONNECTION
TO '...'
USER 'token'
IDENTIFIED BY '$TOKEN';
```

Use the string `token` as username in `USER` and enter your generated token as password in `IDENTIFIED BY`.

##### Service Principal (OAuth M2M)

Create client ID and client secret as described in the [Databricks documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html).

**Not yet implemented** M2M authentication is not yet implemented, see [issue #3](https://github.com/exasol/databricks-virtual-schema/issues/3).

#### Additional Information

You can find additional information about the JDBC connection URL [in the Databricks JDBC driver guide](https://docs.databricks.com/en/_extras/documents/Databricks-JDBC-Driver-Install-and-Configuration-Guide.pdf).

**Important:** Databricks documentation recommends adding the credentials as fields `UID` and `PWD` to the JDBC URL. This may leak credentials in log and error messages. We recommend entering credentials in `USER` and `IDENTIFIED BY` fields of the connection as described above.

### Creating Virtual Schema

```sql
CREATE VIRTUAL SCHEMA VSDAB_VIRTUAL_SCHEMA
    USING VSDAB_SCHEMA.VSDAB_ADAPTER
    WITH
    SCHEMA_NAME     = '<schema name>'
    CATALOG_NAME    = '<Databricks catalog name>'
    SCHEMA_NAME     = '<Databricks schema name>'
```

### Adapter Capabilities

The Exasol optimizer decides on which SQL constructs are pushed-down to Databricks based on the original query and on the capabilities reported by the VSDAB adapter.

VSDAB supports the capabilities listed in the file [`adapter_capabilities.lua`](../../src/main/lua/exasol/adapter/databricks/adapter_capabilities.lua).

#### Excluding Capabilities

Sometimes you want to prevent constructs from being pushed down. In this case, you can tell the RLS adapter to exclude one or more capabilities from being reported to the core database.

The core database will then refrain from pushing down the related SQL constructs.

Just add the property `EXCLUDED_CAPABILITIES` to the Virtual Schema creation statement and provide a comma-separated list of capabilities you want to exclude.

```sql
CREATE VIRTUAL SCHEMA EVSL_VIRTUAL_SCHEMA
    USING VSDAB_SCHEMA.VSDAB_ADAPTER
    WITH
    SCHEMA_NAME           = '<schema name>'
    EXCLUDED_CAPABILITIES = 'SELECTLIST_PROJECTION, ORDER_BY_COLUMN'
```

### Filtering Tables

Often you will not need or even want all the tables in the source schema to be visible in the RLS-protected schema. In those cases you can simply specify an include-list as a property when creating the RLS Virtual Schema.

Just provide a comma-separated list of table names in the property `TABLE_FILTER` and the scan of the source schema will skip all tables that are not listed. In a source schema with a large number of tables, this can also speed up the scan.

```sql
CREATE VIRTUAL SCHEMA EVSL_VIRTUAL_SCHEMA
    USING VSDAB_SCHEMA.VSDAB_ADAPTER
    WITH
    SCHEMA_NAME  = '<schema name>'
    TABLE_FILTER = 'ORDERS, ORDER_ITEMS, PRODUCTS'
```

Spaces around the table names are ignored.

### Changing the Properties of an Existing Virtual Schema

While you could in theory drop and re-create an Virtual Schema, there is a more convenient way to apply changes in the adapter properties.

Use `ALTER VIRTUAL SCHEMA ... SET ...` to update the properties of an existing Virtual Schema.

Example:

```sql
ALTER VIRTUAL SCHEMA EVSL_VIRTUAL_SCHEMA
SET SCHEMA_NAME = '<new schema name>'
```

You can for example change the `SCHEMA_NAME` property to point the Virtual Schema to a new source schema or the [table filter](#filtering-tables).

## Updating a Virtual Schema

All Virtual Schemas cache their metadata. That metadata for example contains all information about structure and data types of the underlying data source. RLS is a Virtual Schema and uses the same caching mechanism.

To let RLS know that something changed in the metadata, please use the [`ALTER VIRTUAL SCHEMA ... REFRESH`](https://docs.exasol.com/sql/alter_schema.htm) statement.

```
ALTER VIRTUAL SCHEMA <virtul schema name> REFRESH
```

Please note that this is also required if you change the special columns that control the RLS protection.


## Using the Virtual Schema

You use Virtual Schemas exactly like you would use a regular schema. The main difference is that they are read-only.

So if you want to query a table in a Virtual Schema, just use the `SELECT` statement.

Example:

```sql
SELECT * FROM EVSL_VIRTUAL_SCHEMA.<table>
```

### Examining the Push-down Query

To understand what a Virtual Schema really does and as a starting point for optimizing your queries, it often helps to take a look at the push-down query Exasol generates. This is as easy as prepending `EXPLAIN VIRTUAL` to your Query.

Example:

```sql
EXPLAIN VIRTUAL SELECT * FROM EVSL_VIRTUAL_SCHEMA.<table>
```

## Known Limitations

* `SELECT *` is not yet supported due to an issue between the core database and the Lua Virtual Schemas in push-down requests (SPOT-10626)
* Source Schema and Virtual Schema must be on the same database.
