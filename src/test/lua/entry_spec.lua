require("busted.runner")()
require("entry")
local http_client = require("exasol.adapter.databricks.http_client")
local log = require("remotelog")
log.set_level("TRACE")

local function http_request_mock(args)
    if args.url == "https://localhost:8888/api/2.1/unity-catalog/catalogs" then
        return [[{"catalogs":[]}]]
    end
    error(string.format("Unknown URL: %s", args.url))
end

describe("entry.adapter_call()", function()
    local original_request = http_client.request
    setup(function()
        ---@diagnostic disable-next-line: duplicate-set-field
        http_client.request = http_request_mock
    end)

    teardown(function()
        http_client.request = original_request
    end)

    it("can call adapter function", function()
        _G.exa = {
            get_connection = function(connection_name)
                return {address = "jdbc:databricks://localhost:8888;PWD=token"}
            end
        }
        adapter_call(
                [[{"type":"createVirtualSchema","schemaMetadataInfo":{"name":"new vs", "properties":{"CONNECTION_NAME":"my_connection"}}}]])
    end)
end)
