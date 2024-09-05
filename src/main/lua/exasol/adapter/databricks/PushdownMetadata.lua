local log = require("remotelog")
local ExaError = require("ExaError")
local TableAdapterNotes = require("exasol.adapter.databricks.TableAdapterNotes")

---@class PushdownMetadata
---@field _table_notes table<string, TableAdapterNotes>
local PushdownMetadata = {}
PushdownMetadata.__index = PushdownMetadata

---@param table_notes table<string, TableAdapterNotes>
---@return PushdownMetadata
function PushdownMetadata:new(table_notes)
    local instance = setmetatable({}, self)
    instance._table_notes = table_notes
    return instance
end

---@param table PushdownInvolvedTable
---@return TableAdapterNotes
local function decode_adapter_notes(table)
    local success, result = pcall(TableAdapterNotes.decode, table.adapterNotes)
    if not success then
        local exa_error = tostring(ExaError:new("E-VSDAB-16",
                                                "Failed to decode adapter notes {{adapter_notes}} for table {{table_name}}.",
                                                {adapter_notes = table.adapterNotes, table_name = table.name})
                :add_mitigations("Please refresh or drop and re-create the virtual schema."))
        log.error(exa_error)
        error(exa_error)
    end
    return result
end

---@param pushdown_request PushdownRequest
---@return PushdownMetadata
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

---@param table_name string
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
