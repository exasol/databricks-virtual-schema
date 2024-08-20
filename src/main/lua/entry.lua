--- Main entry point of the Lua Virtual Schema adapter.
-- It is responsible for creating and wiring up the main adapter objects.
local Adapter = require("exasol.adapter.databricks.Adapter")
local ExasolBaseAdapterProperties = require(
                                        "exasol.evscl.ExasolBaseAdapterProperties")
local MetadataReader = require("exasol.adapter.databricks.MetadataReader")
local RequestDispatcher = require("exasol.vscl.RequestDispatcher")

--- Handle a Virtual Schema request.
-- @param request_as_json JSON-encoded adapter request
-- @return JSON-encoded adapter response
function adapter_call(request_as_json)
    local exasol_context = _G.exa
    local metadata_reader = MetadataReader:new(exasol_context)
    local adapter = Adapter:new(metadata_reader)
    local dispatcher = RequestDispatcher:new(adapter,
                                             ExasolBaseAdapterProperties)
    return dispatcher:adapter_call(request_as_json)
end
