require("exasol_types")
require("exasol.adapter.databricks.common_types")
local log = require("remotelog")
local ExaError = require("ExaError")

--- This class reads details of a Databricks JDBC connection.
---@class ConnectionReader
---@field _exasol_context ExasolUdfContext handle to local database functions and status
local ConnectionReader = {};
ConnectionReader.__index = ConnectionReader;

--- Create a new `ConnectionReader`.
---@param  exasol_context ExasolUdfContext handle to local database functions and status
-- @return connection definition reader
function ConnectionReader:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function ConnectionReader:_init(exasol_context)
    self._exasol_context = exasol_context
end

---Parse the JDBC URL arguments into a table
---@param jdbc_url_args string JDBC URL arguments
---@return table<string, string> arguments
local function parse_args(jdbc_url_args)
    local args = {}
    for k, v in jdbc_url_args:gmatch("([^;=]+)=([^;=]+)") do
        args[k] = v
    end
    return args
end

---Parse the given JDBC URL into its components
---@param jdbc_url string JDBC URL to be parsed
---@return string? host
---@return number? port
---@return string? token
local function parse_jdbc_url(jdbc_url)
    local host, port, jdbc_url_args = jdbc_url:match("jdbc:databricks://([^:]+):(%d+)(.*)")
    local token = nil
    if jdbc_url_args then
        local args = parse_args(jdbc_url_args)
        token = args.PWD
    end
    return host, tonumber(port), token
end

---Read the details for the connection object with the given name
---@param connection_name string name of the connection to be read
---@return DatabricksConnectionDetails connection connection details
function ConnectionReader:read(connection_name)
    log.trace("Reading connection details for '%s'...", connection_name)
    local connection_details = self._exasol_context.get_connection(connection_name)
    if not connection_details then
        error(tostring(ExaError:new("E-VSDAB-2", "Connection '" .. connection_name .. "' not found.")))
    end
    local jdbc_url = connection_details.address
    if not jdbc_url then
        error(tostring(ExaError:new("E-VSDAB-3", "Connection '" .. connection_name .. "' has no address.")))
    end
    local host, port, token = parse_jdbc_url(jdbc_url)
    if not host then
        error(tostring(ExaError:new("E-VSDAB-4", "Connection '" .. connection_name .. "' contains invalid JDBC URL '"
                                            .. jdbc_url .. "'."):add_mitigations(
                "URL must be in the form 'jdbc:databricks://<host>:<port>;PWD=<token>'.")))
    end
    port = port or 443
    local url = "https://" .. host .. ":" .. port
    log.trace("Extracted Databricks URL '%s' from JDBC URL", url)
    return {url = url, token = token}
end

return ConnectionReader;
