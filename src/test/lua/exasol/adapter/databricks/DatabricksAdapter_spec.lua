require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local DatabricksAdapter = require("exasol.adapter.databricks.DatabricksAdapter")
local adapter_capabilities = require("exasol.adapter.databricks.adapter_capabilities")

---@type MetadataReader
local metadata_reader_mock = nil
---@type DatabricksAdapterProperties
local properties_mock = nil

---@return DatabricksAdapter
local function testee()
    return DatabricksAdapter:new(metadata_reader_mock)
end

---@param properties DatabricksAdapterProperties
---@param schema_metadata ExasolSchemaMetadata
local function simulate_metadata(properties, schema_metadata)
    mockagne.when(metadata_reader_mock:read(properties)).thenAnswer(schema_metadata)
end

describe("DatabricksAdapter", function()
    before_each(function()
        metadata_reader_mock = mockagne.getMock()
        properties_mock = mockagne.getMock()
    end)

    describe("create_virtual_schema()", function()
        it("returns createVirtualSchema response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(properties_mock, schema_metadata)
            local response = testee():create_virtual_schema(nil, properties_mock)
            assert.is.same({type = "createVirtualSchema", schemaMetadata = schema_metadata}, response)
        end)
        it("validates properties", function()
            testee():create_virtual_schema(nil, properties_mock)
            mockagne.verify(properties_mock:validate())
        end)
    end)

    describe("set_properties()", function()
        ---@type DatabricksAdapterProperties
        local old_properties_mock = nil
        ---@type DatabricksAdapterProperties
        local new_properties_mock = nil
        ---@type DatabricksAdapterProperties
        local merged_properties_mock = nil

        before_each(function()
            old_properties_mock = mockagne.getMock()
            new_properties_mock = mockagne.getMock()
            merged_properties_mock = mockagne.getMock()
            mockagne.when(old_properties_mock:merge(new_properties_mock)).thenAnswer(merged_properties_mock)
        end)
        it("returns setProperties response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(merged_properties_mock, schema_metadata)
            local response = testee():set_properties(nil, old_properties_mock, new_properties_mock)
            assert.is.same({type = "setProperties", schemaMetadata = schema_metadata}, response)
        end)
        it("validates properties", function()
            testee():set_properties(nil, old_properties_mock, new_properties_mock)
            mockagne.verify(merged_properties_mock:validate())
        end)
    end)

    describe("refresh()", function()
        it("returns refresh response", function()
            local schema_metadata = {tables = {type = "table", name = "virtualTable"}}
            simulate_metadata(properties_mock, schema_metadata)
            local response = testee():refresh(nil, properties_mock)
            assert.is.same({type = "refresh", schemaMetadata = schema_metadata}, response)
        end)
        it("validates properties", function()
            testee():refresh(nil, properties_mock)
            mockagne.verify(properties_mock:validate())
        end)
    end)

    describe("push_down()", function()
        ---@type PushdownRequest
        local request<const> = {
            type = "pushdown",
            involvedTables = {{name = "virtualTable", adapterNotes = "{}", columns = {}}},
            schemaMetadataInfo = {name = "vsName", properties = {}},
            pushdownRequest = {
                type = "select",
                from = {type = "table", name = "virtualTable"},
                selectListDataTypes = {{type = "DECIMAL", precision = 10, scale = 2}}
            }
        }
        it("returns pushdown response", function()
            mockagne.when(properties_mock:get_connection_name()).thenAnswer("conn")
            local response = testee():push_down(request, properties_mock)
            assert.is.same({
                type = "pushdown",
                sql = [[IMPORT INTO (c1 DECIMAL(10,2)) FROM JDBC AT "conn" STATEMENT 'SELECT * FROM `virtualTable`']]
            }, response)
        end)
        it("validates properties", function()
            testee():push_down(request, properties_mock)
            mockagne.verify(properties_mock:validate())
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

    describe("drop_virtual_schema()", function()
        it("returns dropVirtualSchema response", function()
            -- Data type from base library not available
            ---@diagnostic disable-next-line: undefined-field
            local response = testee():drop_virtual_schema(nil, properties_mock)
            assert.is.same({type = "dropVirtualSchema"}, response)
        end)
        it("does not validate properties", function()
            -- Data type from base library not available
            ---@diagnostic disable-next-line: undefined-field
            testee():drop_virtual_schema(nil, properties_mock)
            mockagne.verify_no_call(properties_mock:validate())
        end)
    end)
end)
