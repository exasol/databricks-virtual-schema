require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local PushdownMetadata = require("exasol.adapter.databricks.PushdownMetadata")

---@param involved_tables PushdownInvolvedTable[]
---@return PushdownMetadata
local function create(involved_tables)
    return PushdownMetadata.create({
        pushdownRequest = {type = "select", from = {type = "table", name = "tab1"}, selectListDataTypes = {}},
        schemaMetadataInfo = {name = "schema1", properties = {}},
        type = "pushdown",
        involvedTables = involved_tables
    })
end

describe("PushdownMetadata", function()
    describe("create()", function()
        it("no involved tables", function()
            local actual = create({})
            assert.is.same({}, actual._table_notes)
        end)
        it("missing adapter notes for table", function()
            assert.has_error(function()
                create({{name = "tab1", adapterNotes = nil, columns = {}}})
            end, [[E-VSDAB-15: Adapter notes are missing for table 'tab1'.

Mitigations:

* Please refresh or drop and re-create the virtual schema.]])
        end)
        it("invalid adapter notes for table", function()
            assert.has_error(function()
                create({{name = "tab1", adapterNotes = "invalid json", columns = {}}})
            end, [[E-VSDAB-16: Failed to decode adapter notes 'invalid json' for table 'tab1'.

Mitigations:

* Please refresh or drop and re-create the virtual schema.]])
        end)
        it("empty adapter note json", function()
            local actual = create({{name = "tab1", adapterNotes = "{}", columns = {}}})
            assert.is.same({tab1 = {}}, actual._table_notes)
        end)
        it("filled adapter note json", function()
            local actual = create({
                {name = "tab1", adapterNotes = [[{"catalog_name":"cat1", "schema_name":"schema1"}]], columns = {}}
            })
            assert.is.same({tab1 = {_databricks_catalog = "cat1", _databricks_schema = "schema1"}}, actual._table_notes)
        end)
    end)

    describe("get_table_notes()", function()
        it("missing adapter notes", function()
            local metadata = create({})
            assert.has_error(function()
                metadata:get_table_notes("missing-tab")
            end, [[E-VSDAB-17: Adapter notes are missing for table 'missing-tab'.

Mitigations:

* Please refresh or drop and re-create the virtual schema.]])
        end)
        it("empty adapter notes json", function()
            local metadata = create({{name = "tab1", adapterNotes = "{}", columns = {}}})
            local notes = metadata:get_table_notes("tab1")
            assert.is.same(nil, notes:get_databricks_catalog_name())
            assert.is.same(nil, notes:get_databricks_schema_name())
        end)
        it("gets catalog and schema", function()
            local metadata = create({
                {name = "tab1", adapterNotes = [[{"catalog_name":"cat1", "schema_name":"schema1"}]], columns = {}}
            })
            local notes = metadata:get_table_notes("tab1")
            assert.is.same("cat1", notes:get_databricks_catalog_name())
            assert.is.same("schema1", notes:get_databricks_schema_name())
        end)
    end)
end)
