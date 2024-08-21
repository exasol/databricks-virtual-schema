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

--- Validate the adapter properties.
-- @raise validation error
function DatabricksAdapterProperties:validate()
    AdapterProperties.validate(self) -- super call
end

function DatabricksAdapterProperties:__tostring()
    return AdapterProperties.__tostring(self)
end

return DatabricksAdapterProperties
