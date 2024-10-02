require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local DatabricksRestClient = require("exasol.adapter.databricks.DatabricksRestClient")

log.set_level("INFO")

local function read_databricks_test_config()
    local config = utils.read_test_config()
    return {url = config["databricks.host"], token = config["databricks.token"]}
end

local connection_details = read_databricks_test_config()

local function testee()
    local function token_provider()
        return connection_details.token
    end
    return DatabricksRestClient:new(connection_details.url, token_provider)
end

describe("DatabricksRestClient #itest", function()
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
            assert.is.same("system", columns_table.catalog_name)
            assert.is.same("information_schema", columns_table.schema_name)
            assert.is.same("EXTERNAL", columns_table.table_type)
            assert.is.same("UNITY_CATALOG", columns_table.data_source_format)
            assert.is.same(33, #columns_table.columns)

            local expected_column_metadata = {
                comment = 'Catalog that contains the relation.',
                name = 'table_catalog',
                nullable = false,
                position = 0.0,
                type_json = '{"name":"table_catalog","type":"string","nullable":false,"metadata":{"comment":"Catalog that contains the relation."}}',
                type_name = 'STRING',
                type_precision = 0.0,
                type_scale = 0.0,
                type_text = 'string'
            }
            ---@type DatabricksColumn
            local expected_catalog_column = {
                name = "table_catalog",
                comment = "Catalog that contains the relation.",
                position = 0,
                type = {name = "STRING", text = "string", precision = 0, scale = 0},
                nullable = false,
                databricks_metadata = expected_column_metadata
            }
            assert.is.same(expected_catalog_column, columns_table.columns[1])

            assert.is.same("system.information_schema.columns", columns_table.databricks_metadata.full_name)
            assert.is.same("UNITY_CATALOG", columns_table.databricks_metadata.data_source_format)
        end)

        it("request fails for for unknown catalog/schema", function()
            assert.error_matches(function()
                testee():list_tables("no-such-catalog", "no-such-schema")
                -- Error message will be improved in https://github.com/exasol/databricks-virtual-schema/issues/9
            end, "E%-VSDAB%-5: HTTP request for URL '.*' failed with status 404 %('HTTP/1.1 404 Not Found'%)")
        end)
    end)
end)
