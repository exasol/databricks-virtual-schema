---This class holds parsed adapter notes for a column like the original Databricks column name.
---This information is required for rendering the pushdown query.
---@class ColumnAdapterNotes
---@field _databricks_column string
local ColumnAdapterNotes = {}
ColumnAdapterNotes.__index = ColumnAdapterNotes

---@param databricks_column string Name of the Databricks column
---@return ColumnAdapterNotes
function ColumnAdapterNotes:new(databricks_column)
    local instance = setmetatable({}, self)
    instance._databricks_column = databricks_column
    return instance
end

---@param databricks_columns DatabricksColumn[]
---@return table<string,ColumnAdapterNotes> column_map
function ColumnAdapterNotes.create_map(databricks_columns)
    local columns = {}
    for _, col in pairs(databricks_columns or {}) do
        local name = col.name
        columns[name:upper()] = ColumnAdapterNotes:new(name)
    end
    return columns
end

---@return string databricks_column_name Databricks column name
function ColumnAdapterNotes:get_databricks_column_name()
    return self._databricks_column
end

---@return table<string,any> json_representation JSON representation of the column adapter notes
function ColumnAdapterNotes:_to_json_object()
    return {column_name = self._databricks_column}
end

---@param column_map table<string,ColumnAdapterNotes> columns to convert
---@return table<string,any> json_object_representation
function ColumnAdapterNotes.to_json_map(column_map)
    local result = {}
    for col_name, col in pairs(column_map) do
        result[col_name] = col:_to_json_object()
    end
    return result
end

---@param json_map table<string,any> json_object_representation
---@return table<string,ColumnAdapterNotes> column_map columns
function ColumnAdapterNotes.from_json_map(json_map)
    local result = {}
    for col_name, col in pairs(json_map or {}) do
        result[col_name] = ColumnAdapterNotes:new(col.column_name)
    end
    return result
end

return ColumnAdapterNotes
