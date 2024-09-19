require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local DatabricksAdapter = require("exasol.adapter.databricks.DatabricksAdapter")
local adapter_capabilities = require("exasol.adapter.databricks.adapter_capabilities")

---@type MetadataReader
local metadata_reader_mock = nil
---@type DatabricksAdapterProperties
local properties_mock = nil
---@type DatabricksAdapterProperties
local new_properties_mock = nil

---@return DatabricksAdapter
local function testee()
    return DatabricksAdapter:new(metadata_reader_mock)
end

---@param schema_metadata ExasolSchemaMetadata
local function simulate_metadata(schema_metadata)
    mockagne.when(metadata_reader_mock:read(properties_mock)).thenAnswer(schema_metadata)
end

describe("DatabricksAdapter", function()
    before_each(function()
        metadata_reader_mock = mockagne.getMock()
        properties_mock = mockagne.getMock()
        new_properties_mock = mockagne.getMock()
        mockagne.when(properties_mock:merge(new_properties_mock)).thenAnswer(properties_mock)
    end)

    describe("create_virtual_schema()", function()
        it("returns createVirtualSchema response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(schema_metadata)
            local response = testee():create_virtual_schema(nil, properties_mock)
            assert.is.same({type = "createVirtualSchema", schemaMetadata = schema_metadata}, response)
        end)
    end)

    describe("set_properties()", function()
        it("returns setProperties response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(schema_metadata)
            local response = testee():set_properties(nil, properties_mock, new_properties_mock)
            assert.is.same({type = "setProperties", schemaMetadata = schema_metadata}, response)
        end)
    end)

    describe("refresh()", function()
        it("returns refresh response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(schema_metadata)
            local response = testee():refresh(nil, properties_mock)
            assert.is.same({type = "refresh", schemaMetadata = schema_metadata}, response)
        end)
    end)

    describe("push_down()", function()
        it("returns pushdown response", function()
            ---@type PushdownRequest
            local request = {
                type = "pushdown",
                involvedTables = {{name = "virtualTable", adapterNotes = "{}", columns = {}}},
                schemaMetadataInfo = {name = "vsName", properties = {}},
                pushdownRequest = {
                    type = "select",
                    from = {type = "table", name = "virtualTable"},
                    selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
                }
            }
            mockagne.when(properties_mock:get_connection_name()).thenAnswer("conn")
            local response = testee():push_down(request, properties_mock)
            assert.is.same({
                type = "pushdown",
                sql = [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "conn" STATEMENT 'SELECT * FROM `virtualTable`']]
            }, response)
        end)
    end)

    describe("get_capabilities()", function()
        it("returns getCapabilities response", function()
            -- Data type from base library not available
            ---@diagnostic disable-next-line: undefined-field
            local response = testee():get_capabilities(nil, properties_mock)
            assert.is.same({type = "getCapabilities", capabilities = adapter_capabilities.get_capabilities()}, response)
        end)
    end)
end)
