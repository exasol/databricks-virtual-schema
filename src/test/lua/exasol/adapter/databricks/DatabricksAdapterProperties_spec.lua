require("busted.runner")()
local assert = require("luassert")
local DatabricksAdapterProperties = require("exasol.adapter.databricks.DatabricksAdapterProperties")

local function validate(raw_properties)
    DatabricksAdapterProperties:new(raw_properties):validate()
end

describe("DatabricksAdapterProperties", function()

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
end)
