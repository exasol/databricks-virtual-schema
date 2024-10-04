---@meta databricks_types
---Details for a Databricks catalog. Catalogs are the first layer in Databrick's three-level namespace (catalog.schema.table). They contain schemas.
---See https://docs.databricks.com/en/catalogs/index.html
---@class DatabricksCatalog
---@field name string Name of the catalog
---@field browse_only boolean If true, the principal can only access selective metadata
---@field full_name string Full name of the catalog
local DatabricksCatalog = {}

---Details for a Databricks table.
---See example data https://docs.databricks.com/api/workspace/tables/list
---@class DatabricksTable
---@field name string Name of the table
---@field catalog_name string Name of the catalog
---@field schema_name string Name of the schema
---@field full_name string Full name of the table incl. catalog and schema
---@field table_type string Type of the table, e.g. `MANAGED`
---@field data_source_format string Data source format, e.g. `DELTA`
---@field comment string Comment of the table
---@field columns DatabricksColumn[] Columns
---@field databricks_metadata table<string,any> Original tabledata provided by the Databricks API
local DatabricksTable = {}

---Details for a Databricks column.
---See example data https://docs.databricks.com/api/workspace/tables/list
---@class DatabricksColumn
---@field name string Name of the column
---@field comment string Comment of the column
---@field position integer Position of the column in the table (zero-based)
---@field type DatabricksType Data type of the column
---@field nullable boolean If true, the column can contain NULL values
---@field databricks_metadata table<string,any> Original column metadata provided by the Databricks API
local DatabricksColumn = {}

---Details for a Databricks type.
---See example data https://docs.databricks.com/api/workspace/tables/list
---@class DatabricksType
---@field name string Type name (uppercase, e.g. STRING, TIMESTAMP, INT, LONG, DECIMAL, INTERVAL)
---@field text string Detailed type text (e.g. string, timestamp, int, bigint, decimal(3,5), interval day to second)
---@field precision? integer Precision of the type, seems to be always 0
---@field scale? integer Scale of the type, seems to be always 0
local DatabricksType = {}

---Databricks OAuth token response
---See example data https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html#manually-generate-and-use-access-tokens-for-oauth-m2m-authentication
---@class DatabricksTokenResponse
---@field access_token string Token
---@field token_type string Token type, e.g. `Bearer`
---@field expires_in integer Expiration time
