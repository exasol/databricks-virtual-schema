--- Main entry point of the Lua Virtual Schema adapter.
-- It is responsible for creating and wiring up the main adapter objects.
local DatabricksAdapter = require("exasol.adapter.databricks.DatabricksAdapter")
local DatabricksRestClient = require("exasol.adapter.databricks.DatabricksRestClient")
local DatabricksAdapterProperties = require("exasol.adapter.databricks.DatabricksAdapterProperties")
local MetadataReader = require("exasol.adapter.databricks.MetadataReader")
local RequestDispatcher = require("exasol.vscl.RequestDispatcher")

---@param config DatabricksConnectionDetails
---@return DatabricksRestClient
local function databricks_client_factory(config)
    return DatabricksRestClient:new(config.url, config.token)
end

--- Handle a Virtual Schema request.
---@param  request_as_json string adapter request
---@return string response JSON-encoded adapter response
---@diagnostic disable-next-line: lowercase-global
function adapter_call(request_as_json)
    local exasol_context = _G.exa
    local metadata_reader = MetadataReader:new(exasol_context, databricks_client_factory)
    local adapter = DatabricksAdapter:new(metadata_reader)
    local dispatcher = RequestDispatcher:new(adapter, DatabricksAdapterProperties)
    return dispatcher:adapter_call(request_as_json)
end
