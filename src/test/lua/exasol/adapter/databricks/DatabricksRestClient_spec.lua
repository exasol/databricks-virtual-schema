require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local DatabricksRestClient = require("exasol.adapter.databricks.DatabricksRestClient")

log.set_level("DEBUG")

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
    describe("list_tables()", function()
        it("should be able to list tables", function()
            local tables = testee():list_tables("system", "information_schema")
            assert.is_table(tables)
            assert.is_true(#tables > 0)
            ---@type DatabricksTable
            local columns_table = utils.find_first(tables, function(table)
                return table.name == "columns"
            end)
            assert.is.same("columns", columns_table.name)
            assert.is.same("system.information_schema.columns", columns_table.full_name)
            assert.is.same("Describes columns of tables and views in the catalog.", columns_table.comment)
            assert.is.same(33, #columns_table.columns)
            ---@type DatabricksColumn
            local expected_catalog_column = {
                name = "table_catalog",
                comment = "Catalog that contains the relation.",
                position = 0,
                type = {name = "STRING", text = "string", precision = 0, scale = 0},
                nullable = false
            }
            assert.is.same(expected_catalog_column, columns_table.columns[1])
            ---@type DatabricksColumn
            local expected_position_column = {
                name = "ordinal_position",
                comment = "The position (numbered from 1) of the column within the relation.",
                position = 4,
                type = {name = "INT", text = "int", precision = 0, scale = 0},
                nullable = false
            }
            assert.is.same(expected_position_column, columns_table.columns[5])
            ---@type DatabricksColumn
            local expected_max_length_column = {
                name = "character_maximum_length",
                comment = "Always NULL, reserved for future use.",
                position = 9,
                type = {name = "LONG", text = "long", precision = 0, scale = 0},
                nullable = true
            }
            assert.is.same(expected_max_length_column, columns_table.columns[10])
        end)

        it("request fails for for unknown catalog/schema", function()
            assert.error_matches(function()
                testee():list_tables("no-such-catalog", "no-such-schema")
                -- Error message will be improved in https://github.com/exasol/databricks-virtual-schema/issues/9
            end, "E%-VSDAB%-5: HTTP request for URL '.*' failed with status 404 %('HTTP/1.1 404 Not Found'%)")
        end)
    end)
end)
