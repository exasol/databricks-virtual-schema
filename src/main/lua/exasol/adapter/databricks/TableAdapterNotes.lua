local log = require("remotelog")
local cjson = require("cjson")

---@class TableAdapterNotes
---@field _databricks_catalog string
---@field _databricks_schema string
local TableAdapterNotes = {}
TableAdapterNotes.__index = TableAdapterNotes

---@param databricks_catalog string Name of the Databricks catalog containing the table's schema
---@param databricks_schema string Name of the Databricks schema containing the table
---@return TableAdapterNotes
function TableAdapterNotes:new(databricks_catalog, databricks_schema)
    local instance = setmetatable({}, self)
    instance._databricks_catalog = databricks_catalog
    instance._databricks_schema = databricks_schema
    return instance
end

---@param databricks_table DatabricksTable
function TableAdapterNotes.create(databricks_table)
    log.debug("Creating TableAdapterNotes for table '%s' @ %s.%s", databricks_table.name, databricks_table.catalog_name,
              databricks_table.schema_name)
    return TableAdapterNotes:new(databricks_table.catalog_name, databricks_table.schema_name)
end

---@param adapter_notes string
function TableAdapterNotes.decode(adapter_notes)
    local properties = cjson.decode(adapter_notes)
    return TableAdapterNotes:new(properties.catalog_name, properties.schema_name)
end

---@return string databricks_catalog_name Databricks catalog name including catalog
function TableAdapterNotes:get_databricks_catalog_name()
    return self._databricks_catalog
end

---@return string databricks_schema_name Databricks schema name including catalog
function TableAdapterNotes:get_databricks_schema_name()
    return self._databricks_schema
end

---@return string json_representation
function TableAdapterNotes:to_json()
    return cjson.encode({catalog_name = self._databricks_catalog, schema_name = self._databricks_schema})
end

return TableAdapterNotes
