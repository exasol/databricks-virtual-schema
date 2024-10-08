require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local DatabricksRestClient = require("exasol.adapter.databricks.DatabricksRestClient")
local authentication = require("exasol.adapter.databricks.authentication")

log.set_level("INFO")

local function read_databricks_test_config()
    local config = utils.read_test_config()
    return {
        url = config["databricks.host"],
        token = config["databricks.token"],
        client_id = config["databricks.oauth.clientId"],
        client_secret = config["databricks.oauth.secret"]
    }
end

local test_config = read_databricks_test_config()

local function token_auth_provider()
    return test_config.token
end

local function m2m_auth_provider()
    local provider = authentication.create_token_provider({
        url = test_config.url,
        auth = "m2m",
        oauth_client_id = test_config.client_id,
        oauth_client_secret = test_config.client_secret
    })
    return provider()
end

local function testee_with_token_auth()
    return DatabricksRestClient:new(test_config.url, test_config.token)
end

describe("DatabricksRestClient #itest", function()
    describe("list_tables()", function()
        describe("should be able to list tables with", function()
            local tests = {
                {name = "token auth", token_provider = token_auth_provider},
                {name = "m2m auth", token_provider = m2m_auth_provider}
            }
            for _, test in ipairs(tests) do
                it(test.name, function()
                    local client = DatabricksRestClient:new(test_config.url, test.token_provider())
                    local tables = client:list_tables("system", "information_schema")
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
            end
        end)

        it("request fails for for unknown catalog/schema", function()
            assert.error_matches(function()
                testee_with_token_auth():list_tables("no-such-catalog", "no-such-schema")
                -- Error message will be improved in https://github.com/exasol/databricks-virtual-schema/issues/9
            end, "E%-VSDAB%-5: HTTP request for URL '.*' failed with status 404 %('HTTP/1.1 404 Not Found'%)")
        end)
    end)
end)
