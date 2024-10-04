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
---@return ConnectionReader connection_definition_reader
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
local function parse_properties(jdbc_url_args)
    local args = {}
    if jdbc_url_args then
        for k, v in jdbc_url_args:gmatch("([^;=%s]+)%s*=([^;=]+)") do
            args[k] = v
        end
    end
    return args
end

---Parse the given JDBC URL into its components
---@param jdbc_url string JDBC URL to be parsed
---@return string? host
---@return number? port
---@return table<string,string> jdbc_url_properties
function ConnectionReader._parse_jdbc_url(jdbc_url)
    if not jdbc_url then
        return nil, nil, {}
    end
    local host, port, jdbc_url_args = jdbc_url:match("^jdbc:databricks://([^:]+):(%d+)(.*)$")
    return host, tonumber(port), parse_properties(jdbc_url_args)
end

---Create connection info for M2M OAuth authentication
---@param connection_name string
---@param connection_details Connection
---@param url string
---@param url_properties table<string,string>
---@return DatabricksConnectionDetails
local function m2m_auth_credentials(connection_name, connection_details, url, url_properties)
end

---Create connection info for token authentication
---@param connection_name string
---@param connection_details Connection
---@param url string
---@param url_properties table<string,string>
---@return DatabricksConnectionDetails
local function token_auth_credentials(connection_name, connection_details, url, url_properties)
    if connection_details.user ~= "token" then
        error(tostring(ExaError:new("E-VSDAB-13", "Connection {{connection_name}} contains invalid user {{user}}.",
                                    {connection_name = connection_name, user = connection_details.user})
                :add_mitigations(
                        "Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.")))
    end
    if not connection_details.password then
        error(tostring(ExaError:new("E-VSDAB-14", "Connection {{connection_name}} does not contain a valid token.",
                                    {connection_name = connection_name}):add_mitigations(
                "Please specify PASSWORD='<token>` in the connection.")))
    end
    log.trace("Extracted Databricks URL '%s' from JDBC URL", url)
    return {url = url, token = connection_details.password}
end

---Read the details for the connection object with the given name
---@param connection_name string name of the connection to be read
---@return DatabricksConnectionDetails connection connection details
function ConnectionReader:read(connection_name)
    log.trace("Reading connection details for '%s'...", connection_name)
    local connection_details = self._exasol_context.get_connection(connection_name)
    if not connection_details then
        error(tostring(ExaError:new("E-VSDAB-2", "Connection {{connection_name}} not found.",
                                    {connection_name = connection_name})))
    end
    local jdbc_url = connection_details.address
    if not jdbc_url then
        error(tostring(ExaError:new("E-VSDAB-3", "Connection {{connection_name}} has no address.",
                                    {connection_name = connection_name})))
    end
    local host, port, url_properties = ConnectionReader._parse_jdbc_url(jdbc_url)
    if not host then
        error(tostring(ExaError:new("E-VSDAB-4",
                                    "Connection {{connection_name}} contains invalid JDBC URL {{jdbc_url}}.",
                                    {connection_name = connection_name, jdbc_url = jdbc_url}):add_mitigations(
                "URL must be in the form 'jdbc:databricks://<host>:<port>;parameter=value;...'.")))
    end
    port = port or 443
    local url = "https://" .. host .. ":" .. port

    ---@type string
    local auth_mech = url_properties.AuthMech
    if not auth_mech then
        error(tostring(ExaError:new("E-VSDAB-21",
                                    "Connection {{connection_name}} contains JDBC URL {{jdbc_url}} without AuthMech property.",
                                    {connection_name = connection_name, jdbc_url = jdbc_url, auth_mech = auth_mech})
                :add_mitigations("Specify one of the supported AuthMech values 3 (token auth) or 11 (M2M OAuth).")))
    elseif auth_mech == "3" then
        return token_auth_credentials(connection_name, connection_details, url, url_properties)
    elseif auth_mech == "11" then
        return m2m_auth_credentials(connection_name, connection_details, url, url_properties)
    else
        error(tostring(ExaError:new("E-VSDAB-22",
                                    "Connection {{connection_name}} contains JDBC URL {{jdbc_url}} with unsupported AuthMech property value {{auth_mech}}.",
                                    {connection_name = connection_name, jdbc_url = jdbc_url, auth_mech = auth_mech})
                :add_mitigations("Use one of the supported AuthMech values 3 (token auth) or 11 (M2M OAuth).")))
    end
end

return ConnectionReader;
