require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local log = require("remotelog")
local DatabricksQueryRewriter = require("exasol.adapter.databricks.DatabricksQueryRewriter")

log.set_level("INFO")

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

local function simulate_table_notes(table_name, databricks_catalog_name, databricks_schema_name, databricks_table_name)
    ---@type TableAdapterNotes
    local table_adapter_notes_mock = mockagne.getMock()
    mockagne.when(table_adapter_notes_mock:get_databricks_catalog_name()).thenAnswer(databricks_catalog_name)
    mockagne.when(table_adapter_notes_mock:get_databricks_schema_name()).thenAnswer(databricks_schema_name)
    mockagne.when(table_adapter_notes_mock:get_databricks_table_name()).thenAnswer(databricks_table_name)
    mockagne.when(metadata_mock:get_table_notes(table_name)).thenAnswer(table_adapter_notes_mock)
end

local function simulate_column_name(exasol_table_name, exasol_column_name, databricks_column_name)
    local mock = metadata_mock:get_table_notes(exasol_table_name)
    mockagne.when(mock:get_databricks_column_name(exasol_column_name)).thenAnswer(databricks_column_name)
end

describe("DatabricksQueryRewriter", function()
    describe("rewrite()", function()
        it("renders empty query", function()
            assert_rewritten({}, [[IMPORT FROM JDBC AT "connection_id" STATEMENT 'SELECT *']])
        end)
        it("renders invalid object", function()
            assert_rewritten("invalid", [[IMPORT FROM JDBC AT "connection_id" STATEMENT 'SELECT *']])
        end)
        it("renders query without columns", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema", "databricks-table")
            assert_rewritten({type = "select", from = {type = "table", name = "tab1"}, selectListDataTypes = {}},
                             [[IMPORT FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`databricks-table`']])
        end)
        it("renders query with table", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema", "databricks-table")
            local original_query = {
                type = "select",
                from = {type = "table", name = "tab1"},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
            assert_rewritten(original_query,
                             [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`databricks-table`']])
        end)
        it("overwrites existing schema", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema", "databricks-table")
            local original_query = {
                type = "select",
                from = {type = "table", name = "tab1", schema = "schema1"},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
            assert_rewritten(original_query,
                             [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "connection_id" STATEMENT 'SELECT * FROM `databricks-catalog`.`databricks-schema`.`databricks-table`']])
        end)
        it("overwrites table in column expression", function()
            simulate_table_notes("tab1", "databricks-catalog", "databricks-schema", "databricks-table")
            simulate_column_name("tab1", "col", "databricks-col")
            local original_query = {
                type = "select",
                from = {type = "table", name = "tab1", schema = "schema1"},
                selectList = {{type = "column", columnNr = 0, name = "col", tableName = "tab1"}},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
            assert_rewritten(original_query,
                             [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "connection_id" STATEMENT 'SELECT `databricks-table`.`databricks-col` FROM `databricks-catalog`.`databricks-schema`.`databricks-table`']])
        end)
    end)
end)
