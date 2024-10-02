local log = require("remotelog")
local ExaError = require("ExaError")
local TableAdapterNotes = require("exasol.adapter.databricks.TableAdapterNotes")

---This class holds metadata about the virtual schema that is required for rendering the pushdown query.
---It allows convenient access to adapter notes of tables.
---@class PushdownMetadata
---@field _table_notes table<string, TableAdapterNotes> metadata describing databricks tables
local PushdownMetadata = {}
PushdownMetadata.__index = PushdownMetadata

---@param table_notes table<string, TableAdapterNotes>
---@return PushdownMetadata
function PushdownMetadata:new(table_notes)
    local instance = setmetatable({}, self)
    instance._table_notes = table_notes
    return instance
end

---@param table PushdownInvolvedTable databricks table for which the adapter notes should be decoded
---@return TableAdapterNotes adapter notes as Lua table
local function decode_adapter_notes(table)
    local success, result = pcall(TableAdapterNotes.decode, table.adapterNotes)
    if not success then
        local exa_error = tostring(ExaError:new("E-VSDAB-16",
                                                "Failed to decode adapter notes {{adapter_notes}} for table {{table_name}}: {{error_msg}}",
                                                {
            adapter_notes = table.adapterNotes,
            table_name = table.name,
            error_msg = result
        }):add_mitigations("Please refresh or drop and re-create the virtual schema."))
        log.error(exa_error)
        error(exa_error)
    end
    return result
end

---@param pushdown_request PushdownRequest virtual schema push-down request
---@return PushdownMetadata metadata to be attached to the push-down request
function PushdownMetadata.create(pushdown_request)
    ---@type table<string, TableAdapterNotes>
    local table_notes = {}
    for _, table in ipairs(pushdown_request.involvedTables) do
        if table.adapterNotes == nil then
            local exa_error = tostring(ExaError:new("E-VSDAB-15", "Adapter notes are missing for table {{table_name}}.",
                                                    {table_name = table.name}):add_mitigations(
                    "Please refresh or drop and re-create the virtual schema."))
            log.error(exa_error)
            error(exa_error)
        end
        table_notes[table.name] = decode_adapter_notes(table)
    end
    return PushdownMetadata:new(table_notes)
end

---Get adapter notes for the table with the given name.
---@param table_name string table for which to get the adapter notes
---@return TableAdapterNotes
function PushdownMetadata:get_table_notes(table_name)
    local notes = self._table_notes[table_name]
    if notes then
        return notes
    end
    local exa_error = tostring(ExaError:new("E-VSDAB-17", "Adapter notes are missing for table {{table_name}}.",
                                            {table_name = table_name}):add_mitigations(
            "Please refresh or drop and re-create the virtual schema."))
    log.error(exa_error)
    error(exa_error)
end

return PushdownMetadata
