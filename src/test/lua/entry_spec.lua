require("busted.runner")()
require("entry")
local http_client = require("exasol.adapter.databricks.http_client")
local log = require("remotelog")
local util = require("exasol.adapter.databricks.test_utils")
log.set_level("INFO")

local function http_request_mock(args)
    if args.url
            == "https://localhost:8888/api/2.1/unity-catalog/tables?catalog_name=catalog&schema_name=schema&max_results=50&include_delta_metadata=true&omit_columns=false&omit_properties=true&include_browse=false" then
        return [[{"tables":[]}]]
    end
    error(string.format("Unknown URL: %s", args.url))
end

local function create_exa_context_mock()
    return {
        get_connection = function(connection_name)
            if connection_name == "my_connection" then
                return {address = "jdbc:databricks://localhost:8888;arg=value", user = "token", password = "token"}
            end
            error(string.format("Unknown connection name: '%s'", connection_name))
        end
    }
end

describe("entry.adapter_call()", function()
    local original_request = http_client.request
    setup(function()
        ---@diagnostic disable-next-line: duplicate-set-field
        http_client.request = http_request_mock
        _G.exa = create_exa_context_mock()
    end)

    teardown(function()
        http_client.request = original_request
        _G.exa = nil
    end)

    it("can call adapter function", function()
        local actual = adapter_call(
                [[{"type":"createVirtualSchema","schemaMetadataInfo":{"name":"new vs", "properties":{
                "CONNECTION_NAME":"my_connection", "CATALOG_NAME":"catalog", "SCHEMA_NAME": "schema"
            }}}]])
        util.assert_json_same({type = "createVirtualSchema", schemaMetadata = {tables = {}, adapterNotes = "notes"}},
                              actual)
    end)
end)
