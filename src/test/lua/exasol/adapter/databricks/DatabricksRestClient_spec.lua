require("busted.runner")()
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local DatabricksRestClient = require("exasol.adapter.databricks.DatabricksRestClient")

log.set_level("TRACE")

local function read_databricks_test_config()
    local config = utils.read_test_config()
    return {url = config["databricks.host"], token = config["databricks.token"]}
end

local connection_details = read_databricks_test_config()

local function testee()
    return DatabricksRestClient:new(connection_details)
end

describe("DatabricksRestClient #itest", function()
    describe("_get_request()", function()
        it("should handle error response", function()
            assert.has_error(function()
                ---@diagnostic disable-next-line: invisible
                testee():_get_request("/invalid")
            end, "E-VSDAB-5: HTTP request for URL '" .. connection_details.url .. "/invalid"
                                     .. "' failed with status 404 ('HTTP/1.1 404 Not Found') and body ''")
        end)
    end)

    describe("list_catalogs()", function()
        it("should be able to list catalogs", function()
            local catalogs = testee():list_catalogs()
            assert.is_table(catalogs)
            assert.is_true(#catalogs > 0)
            local system_catalog = utils.find_first(catalogs, function(catalog)
                return catalog.name == "system"
            end)
            assert.is.same({name = "system", browse_only = false, full_name = "system"}, system_catalog)
        end)
    end)
end)
