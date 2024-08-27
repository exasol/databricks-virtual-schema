require("busted.runner")()
local assert = require("luassert")
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
