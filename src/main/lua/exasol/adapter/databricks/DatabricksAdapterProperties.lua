local ExaError = require("ExaError")
local log = require("remotelog")

---This class abstracts access to the user-defined properties of the Virtual Schema.
---@class DatabricksAdapterProperties
local DatabricksAdapterProperties = {}
DatabricksAdapterProperties.__index = DatabricksAdapterProperties
local AdapterProperties = require("exasol.vscl.AdapterProperties")
setmetatable(DatabricksAdapterProperties, AdapterProperties)

---Create a new `ExasolAdapterProperties` instance
---@param raw_properties any unparsed user-defined properties
---@return DatabricksAdapterProperties new instance of the Databricks Virtual Schema adapter properties
function DatabricksAdapterProperties:new(raw_properties)
    local instance = setmetatable({}, self)
    instance:_init(raw_properties)
    return instance
end

function DatabricksAdapterProperties:_init(raw_properties)
    AdapterProperties._init(self, raw_properties)
end

---Get the class of the object
---@return DatabricksAdapterProperties class
function DatabricksAdapterProperties:class()
    return DatabricksAdapterProperties
end

local CONNECTION_NAME_PROPERTY<const> = "CONNECTION_NAME"
local CATALOG_NAME_PROPERTY<const> = "CATALOG_NAME"
local SCHEMA_NAME_PROPERTY<const> = "SCHEMA_NAME"

local MANDATORY_PROPERTY_NAMES<const> = {CONNECTION_NAME_PROPERTY, CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY}

---Validate the adapter properties.
---@raise validation error
function DatabricksAdapterProperties:validate()
    AdapterProperties.validate(self) -- super call
    for _, property_name in ipairs(MANDATORY_PROPERTY_NAMES) do
        self:_validate_mandatory_property(property_name)
    end
end

---Verify that a property with the given name is present.
---@param property_name string
function DatabricksAdapterProperties:_validate_mandatory_property(property_name)
    ---@diagnostic disable-next-line: undefined-field # Type annotations for library not available
    if not self:has_value(property_name) then
        local mitigation = "Specify the '" .. property_name .. "' property in the CREATE VIRTUAL SCHEMA statement."
        ExaError:new("F-VSDAB-1", "Property '" .. property_name .. "' is missing"):add_mitigations(mitigation):raise(0)
    end
end

---Get the name of the database object that defines the parameter of the connection to the remote data source.
---@param property_name string 
---@return string property_value
function DatabricksAdapterProperties:_get_mandatory_field(property_name)
    ---@diagnostic disable-next-line: undefined-field # Type annotations for library not available
    return self:get(property_name)
end

---Get the name of the database object that defines the parameter of the connection to the remote data source.
---@return string connection_name
function DatabricksAdapterProperties:get_connection_name()
    return self:_get_mandatory_field(CONNECTION_NAME_PROPERTY)
end

---Get the name of the Databricks catalog for which to create the virtual schema.
---@return string catalog_name
function DatabricksAdapterProperties:get_catalog_name()
    return self:_get_mandatory_field(CATALOG_NAME_PROPERTY)
end

---Get the name of the Databricks schema for which to create the virtual schema.
---@return string schema_name
function DatabricksAdapterProperties:get_schema_name()
    return self:_get_mandatory_field(SCHEMA_NAME_PROPERTY)
end

function DatabricksAdapterProperties:__tostring()
    return AdapterProperties.__tostring(self)
end

return DatabricksAdapterProperties
