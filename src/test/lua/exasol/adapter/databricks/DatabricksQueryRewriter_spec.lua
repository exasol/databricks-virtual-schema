require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local DatabricksQueryRewriter = require("exasol.adapter.databricks.DatabricksQueryRewriter")

---@type PushdownMetadata
local metadata_mock = nil
before_each(function()
    metadata_mock = mockagne.getMock()
end)

local function rewrite(original_query)
    local rewriter = DatabricksQueryRewriter:new("connection_id", metadata_mock)
    return rewriter:rewrite(original_query)
end

local function assert_rewritten(original_query, expected_import_statement)
    assert.is.same(expected_import_statement, rewrite(original_query))
end

local function simulate_table_notes(table_name, catalog_name, schema_name)
    ---@type TableAdapterNotes
    local table_adapter_notes_mock = mockagne.getMock()
    mockagne.when(table_adapter_notes_mock:get_databricks_catalog_name()).thenAnswer(catalog_name)
    mockagne.when(table_adapter_notes_mock:get_databricks_schema_name()).thenAnswer(schema_name)
    mockagne.when(metadata_mock:get_table_notes(table_name)).thenAnswer(table_adapter_notes_mock)
end

describe("DatabricksQueryRewriter", function()
    describe("rewrite()", function()
        it("renders empty query", function()
            assert_rewritten({}, [[IMPORT FROM JDBC AT "connection_id" STATEMENT 'SELECT *']])
        end)
        it("renders query without columns", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema")
            assert_rewritten({type = "select", from = {type = "table", name = "tab1"}, selectListDataTypes = {}},
                             [[IMPORT FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`tab1`']])
        end)
        it("renders query with table", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema")
            local original_query = {
                type = "select",
                from = {type = "table", name = "tab1"},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
            assert_rewritten(original_query,
                             [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`tab1`']])
        end)
        it("overwrites existing schema", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema")
            local original_query = {
                type = "select",
                from = {type = "table", name = "tab1", schema = "schema1"},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
            assert_rewritten(original_query,
                             [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`tab1`']])
        end)
    end)
end)
