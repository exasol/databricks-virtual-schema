---@diagnostic disable: missing-fields # We don't want to specify unnecessary fields in tests
require("busted.runner")()
require("exasol.assertions")
local assert = require("luassert")
local log = require("remotelog")
local TableAdapterNotes = require("exasol.adapter.databricks.TableAdapterNotes")

log.set_level("INFO")

---@param databricks_table DatabricksTable
---@return TableAdapterNotes
local function create(databricks_table)
    return TableAdapterNotes.create(databricks_table)
end

describe("TableAdapterNotes", function()
    describe("get_databricks_catalog_name()", function()
        it("returns nil when value is missing", function()
            assert.is.same(nil, create({}):get_databricks_catalog_name())
        end)
        it("returns value", function()
            assert.is.same("cat", create({catalog_name = "cat"}):get_databricks_catalog_name())
        end)
    end)

    describe("get_databricks_schema_name()", function()
        it("returns nil when value is missing", function()
            assert.is.same(nil, create({}):get_databricks_schema_name())
        end)
        it("returns value", function()
            assert.is.same("schema", create({schema_name = "schema"}):get_databricks_schema_name())
        end)
    end)

    describe("get_databricks_table_name()", function()
        it("returns nil when value is missing", function()
            assert.is.same(nil, create({}):get_databricks_table_name())
        end)
        it("returns value", function()
            assert.is.same("tab", create({name = "tab"}):get_databricks_table_name())
        end)
    end)

    describe("get_databricks_column_name()", function()
        it("fails when column not found", function()
            local notes = create({})
            assert.has_error(function()
                notes:get_databricks_column_name("missing")
            end, [[E-VSDAB-18: Column notes not found for Exasol column 'missing'.

Mitigations:

* Please refresh or drop and re-create the virtual schema.]])
        end)
        it("returns column name", function()
            local notes = create({columns = {{name = "col"}}})
            assert.is.same("col", notes:get_databricks_column_name("COL"))
        end)
    end)

    describe("to_json()", function()
        it("returns json for empty notes", function()
            local json = create({}):to_json()
            assert.is.same('{"columns":{}}', json)
        end)
        it("returns json for empty columns map", function()
            local json = create({columns = {}}):to_json()
            assert.is.same('{"columns":{}}', json)
        end)
        it("returns json for filled", function()
            local json = create({
                catalog_name = "cat",
                schema_name = "schema",
                name = "tab",
                columns = {{name = "col1"}, {name = "col2"}}
            }):to_json()
            assert.is.same_json({
                catalog_name = "cat",
                schema_name = "schema",
                table_name = "tab",
                columns = {COL1 = {column_name = "col1"}, COL2 = {column_name = "col2"}}
            }, json)
        end)
    end)
end)
