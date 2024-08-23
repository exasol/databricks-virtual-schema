---@meta exasol_types
---Context for Exasol Lua UDFs.
---@class ExasolUdfContext
local ExasolUdfContext = {}
---Get the connection details for the named connection.
---@param connection_name string The name of the connection.
---@return Connection? connection connection details.
function ExasolUdfContext.get_connection(connection_name)
end
---An Exasol connection
---@class Connection
---@field address string? The address of the connection.
---@field user string? The user name for the connection.
---@field password string? The password for the connection.
