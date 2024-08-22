local ExaError = require("ExaError")
local log = require("remotelog")

--- This class abstracts access to the user-defined properties of the Virtual Schema.
-- @classmod DatabricksAdapterProperties
local DatabricksAdapterProperties = {}
DatabricksAdapterProperties.__index = DatabricksAdapterProperties
local AdapterProperties = require("exasol.vscl.AdapterProperties")
setmetatable(DatabricksAdapterProperties, AdapterProperties)

--- Create a new `ExasolAdapterProperties` instance
-- @param raw_properties unparsed user-defined properties
-- @return new instance
function DatabricksAdapterProperties:new(raw_properties)
    local instance = setmetatable({}, self)
    instance:_init(raw_properties)
    return instance
end

function DatabricksAdapterProperties:_init(raw_properties)
    AdapterProperties._init(self, raw_properties)
end

--- Get the class of the object
-- @return class
function DatabricksAdapterProperties:class()
    return DatabricksAdapterProperties
end

local CONNECTION_NAME_PROPERTY<const> = "CONNECTION_NAME"

--- Validate the adapter properties.
-- @raise validation error
function DatabricksAdapterProperties:validate()
    AdapterProperties.validate(self) -- super call
    if not self:has_value(CONNECTION_NAME_PROPERTY) then
        ExaError:new("F-VSDAB-1", "Property '" .. CONNECTION_NAME_PROPERTY .. "' is missing"):add_mitigations(
                "Specify the '" .. CONNECTION_NAME_PROPERTY .. ' property in the CREATE VIRTUAL SCHEMA statement.')
                :raise(0)
    end
end

--- Get the name of the database object that defines the parameter of the connection to the remote data source.
-- @return name of the connection object
function DatabricksAdapterProperties:get_connection_name()
    return self:get(CONNECTION_NAME_PROPERTY)
end

function DatabricksAdapterProperties:__tostring()
    return AdapterProperties.__tostring(self)
end

return DatabricksAdapterProperties
