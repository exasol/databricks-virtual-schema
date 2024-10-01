local log = require("remotelog")
local cjson = require("cjson")

---This class holds parsed adapter notes for a table like the Databricks catalog and schema names.
---This information is required for rendering the pushdown query.
---@class TableAdapterNotes
---@field _databricks_catalog string
---@field _databricks_schema string
---@field _databricks_metadata table<string,any>
local TableAdapterNotes = {}
TableAdapterNotes.__index = TableAdapterNotes

---@param databricks_catalog string Name of the Databricks catalog containing the table's schema
---@param databricks_schema string Name of the Databricks schema containing the table
---@param databricks_metadata table<string,any> Optional original Databricks table metadata.
---@return TableAdapterNotes
function TableAdapterNotes:new(databricks_catalog, databricks_schema, databricks_metadata)
    local instance = setmetatable({}, self)
    instance._databricks_catalog = databricks_catalog
    instance._databricks_schema = databricks_schema
    instance._databricks_metadata = databricks_metadata
    return instance
end

---@param databricks_table DatabricksTable
function TableAdapterNotes.create(databricks_table)
    log.trace("Creating TableAdapterNotes for table '%s' @ %s.%s", databricks_table.name, databricks_table.catalog_name,
              databricks_table.schema_name)
    return TableAdapterNotes:new(databricks_table.catalog_name, databricks_table.schema_name,
                                 databricks_table.databricks_metadata)
end

---@param adapter_notes string adapter notes as JSON string  
---@return TableAdapterNotes Lua object representing the adapter notes for a databricks table  
function TableAdapterNotes.decode(adapter_notes)
    local properties = cjson.decode(adapter_notes)
    return TableAdapterNotes:new(properties.catalog_name, properties.schema_name, properties.databricks_metadata)
end

---@return string databricks_catalog_name Databricks catalog name
function TableAdapterNotes:get_databricks_catalog_name()
    return self._databricks_catalog
end

---@return string databricks_schema_name Databricks schema name
function TableAdapterNotes:get_databricks_schema_name()
    return self._databricks_schema
end

---@return string json_representation JSON representation of the table adapter notes
function TableAdapterNotes:to_json()
    return cjson.encode({
        catalog_name = self._databricks_catalog,
        schema_name = self._databricks_schema,
        databricks_metadata = self._databricks_metadata
    })
end

return TableAdapterNotes
