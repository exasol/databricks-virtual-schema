local http_client = require("exasol.adapter.databricks.http_client")
local base64 = require("exasol.adapter.databricks.base64")
local ExaError = require("ExaError")
local cjson = require("cjson")
local log = require("remotelog")

local M = {}

---@alias TokenProvider fun(): string

---@param connection_details DatabricksConnectionDetails
---@return string
local function fetch_oauth_token(connection_details)
    log.trace("Fetching new OAuth M2M token")
    local body = http_client.request({
        url = connection_details.url .. "/oidc/v1/token",
        method = "POST",
        headers = {
            ["Content-Type"] = "application/x-www-form-urlencoded",
            Authorization = "Basic "
                    .. base64.encode(
                            string.format("%s:%s", connection_details.oauth_client_id,
                                          connection_details.oauth_client_secret))
        },
        request_body = "grant_type=client_credentials&scope=all-apis",
        verify_tls_certificate = false
    })
    local data = cjson.decode(body)
    return data.access_token
end

---@param connection_details DatabricksConnectionDetails
---@return TokenProvider
local function create_m2m_token_provider(connection_details)
    local token = nil
    return function()
        if token == nil then
            token = fetch_oauth_token(connection_details)
        else
            log.trace("Token already available, no need to fetch it again")
        end
        return token
    end
end

---@param token string bearer token
---@return TokenProvider
local function create_bearer_token_provider(token)
    return function()
        return token
    end
end

---Create a new TokenProvider for the given connection details.
---@param connection_details DatabricksConnectionDetails
---@return TokenProvider token_provider
function M.create_token_provider(connection_details)
    if connection_details.token then
        return create_bearer_token_provider(connection_details.token)
    end
    if connection_details.oauth_client_id and connection_details.oauth_client_secret then
        return create_m2m_token_provider(connection_details)
    end
    local exa_error = tostring(ExaError:new("E-VSDAB-20", "No Databricks credentials found."):add_mitigations(
            "Please provide token or OAuth M2 credentials as specified in the user guide."))
    log.error(exa_error)
    error(exa_error)
end

return M
