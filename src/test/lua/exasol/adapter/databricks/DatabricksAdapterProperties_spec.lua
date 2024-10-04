require("busted.runner")()
local assert = require("luassert")
local DatabricksAdapterProperties = require("exasol.adapter.databricks.DatabricksAdapterProperties")

---@param raw_properties table<string, any> 
---@return DatabricksAdapterProperties properties
local function testee(raw_properties)
    return DatabricksAdapterProperties:new(raw_properties)
end

local function validate(raw_properties)
    testee(raw_properties):validate()
end

describe("DatabricksAdapterProperties", function()
    describe("class()", function()
        it("returns class", function()
            assert.is.equal(DatabricksAdapterProperties, testee({}):class())
        end)
    end)
    describe("__tostring()", function()
        it("returns string representation for empty properties", function()
            assert.is.same("()", tostring(testee({})))
        end)
        it("returns string representation for non-empty properties", function()
            assert.is.same("(CONNECTION_NAME = connection)", tostring(testee({CONNECTION_NAME = "connection"})))
        end)
    end)
    describe("validate()", function()
        it("validation succeeds", function()
            assert.has_no.errors(function()
                validate({CONNECTION_NAME = "connection", CATALOG_NAME = "catalog", SCHEMA_NAME = "schema"})
            end)
        end)
        it("calls validate() from parent class", function()
            assert.has_error(function()
                validate({CONNECTION_NAME = "connection", CATALOG_NAME = "catalog", LOG_LEVEL = "invalid"})
            end, [[F-VSCL-PROP-2: Unknown log level 'invalid' in LOG_LEVEL property

Mitigations:

* Pick one of: FATAL, ERROR, WARNING, INFO, CONFIG, DEBUG, TRACE]])
        end)
        describe("validation fails for missing property", function()
            local tests = {
                {property_name = "CONNECTION_NAME", properties = {}},
                {property_name = "CATALOG_NAME", properties = {CONNECTION_NAME = "connection"}},
                {property_name = "SCHEMA_NAME", properties = {CONNECTION_NAME = "connection", CATALOG_NAME = "catalog"}}
            }
            for _, test in ipairs(tests) do
                it(test.property_name, function()
                    assert.has_error(function()
                        validate(test.properties)
                    end, string.format([[F-VSDAB-1: Property '%s' is missing

Mitigations:

* Specify the '%s' property in the CREATE VIRTUAL SCHEMA statement.]], test.property_name, test.property_name))
                end)
            end
        end)
    end)

    describe("get_connection_name()", function()
        it("returns the connection name", function()
            local properties = testee({CONNECTION_NAME = "connection"})
            assert.is.same("connection", properties:get_connection_name())
        end)
    end)

    describe("get_catalog_name()", function()
        it("returns the catalog name", function()
            local properties = testee({CATALOG_NAME = "catalog"})
            assert.is.same("catalog", properties:get_catalog_name())
        end)
    end)

    describe("get_schema_name()", function()
        it("returns the schema name", function()
            local properties = testee({SCHEMA_NAME = "schema"})
            assert.is.same("schema", properties:get_schema_name())
        end)
    end)
end)
