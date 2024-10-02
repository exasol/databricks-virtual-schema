local log = require("remotelog")
local cjson = require("cjson")
local ExaError = require("ExaError")
local ColumnAdapterNotes = require("exasol.adapter.databricks.ColumnAdapterNotes")

---This class holds parsed adapter notes for a table like the Databricks catalog and schema names.
---This information is required for rendering the pushdown query.
---@class TableAdapterNotes
---@field _databricks_catalog string
---@field _databricks_schema string
---@field _databricks_table string
---@field _databricks_columns table<string,ColumnAdapterNotes>
---@field _databricks_metadata table<string,any>
local TableAdapterNotes = {}
TableAdapterNotes.__index = TableAdapterNotes

---@param databricks_catalog string Name of the Databricks catalog containing the table's schema
---@param databricks_schema string Name of the Databricks schema containing the table
---@param databricks_table string Original name of the Databricks table, might use a different lower/upper case
---@param databricks_columns table<string,ColumnAdapterNotes> Column adapter notes
---@param databricks_metadata table<string,any> Optional original Databricks table metadata.
---@return TableAdapterNotes
function TableAdapterNotes:new(databricks_catalog, databricks_schema, databricks_table, databricks_columns,
        databricks_metadata)
    local instance = setmetatable({}, self)
    instance._databricks_catalog = databricks_catalog
    instance._databricks_schema = databricks_schema
    instance._databricks_table = databricks_table
    instance._databricks_columns = databricks_columns
    instance._databricks_metadata = databricks_metadata
    return instance
end

---@param databricks_table DatabricksTable
function TableAdapterNotes.create(databricks_table)
    log.trace("Creating TableAdapterNotes for table '%s' @ %s.%s", databricks_table.name, databricks_table.catalog_name,
              databricks_table.schema_name)
    ---@type table<string,ColumnAdapterNotes>
    local databricks_columns = {}
    for _, col in pairs(databricks_table.columns) do
        databricks_columns[col.name:upper()] = ColumnAdapterNotes.create(col)
    end
    return TableAdapterNotes:new(databricks_table.catalog_name, databricks_table.schema_name, databricks_table.name,
                                 databricks_columns, databricks_table.databricks_metadata)
end

---@param adapter_notes string adapter notes as JSON string  
---@return TableAdapterNotes Lua object representing the adapter notes for a databricks table  
function TableAdapterNotes.decode(adapter_notes)
    log.trace("Decoding adapter notes %s", adapter_notes)
    local properties = cjson.decode(adapter_notes)
    local columns = ColumnAdapterNotes.from_json_map(properties.columns)
    return TableAdapterNotes:new(properties.catalog_name, properties.schema_name, properties.table_name, columns,
                                 properties.databricks_metadata)
end

---@return string databricks_catalog_name Databricks catalog name
function TableAdapterNotes:get_databricks_catalog_name()
    return self._databricks_catalog
end

---@return string databricks_schema_name Databricks schema name
function TableAdapterNotes:get_databricks_schema_name()
    return self._databricks_schema
end

---@return string databricks_table_name Databricks table name
function TableAdapterNotes:get_databricks_table_name()
    return self._databricks_table
end

---Get the name of the column to which the given Exasol column is mapped.
---@param  exasol_column_name string Exasol column name
---@return string databricks_column_name Databricks column name
function TableAdapterNotes:get_databricks_column_name(exasol_column_name)
    local column_notes = self._databricks_columns[exasol_column_name]
    if not column_notes then
        local exa_error = tostring(ExaError:new("E-VSDAB-18",
                                                "Column notes not found for Exasol column {{column_name}}.",
                                                {column_name = exasol_column_name}):add_mitigations(
                "Please refresh or drop and re-create the virtual schema."))
        log.error(exa_error)
        error(exa_error)
    end
    return column_notes:get_databricks_column_name()
end

---@return string json_representation JSON representation of the table adapter notes
function TableAdapterNotes:to_json()
    return cjson.encode({
        catalog_name = self._databricks_catalog,
        schema_name = self._databricks_schema,
        table_name = self._databricks_table,
        columns = ColumnAdapterNotes.to_json_map(self._databricks_columns),
        databricks_metadata = self._databricks_metadata
    })
end

return TableAdapterNotes
