local http_client = require("exasol.adapter.databricks.http_client")
local util = require("exasol.adapter.databricks.util")
local ExaError = require("ExaError")
local cjson = require("cjson")
local log = require("remotelog")

local M = {}

---@alias TokenProvider fun(): string

---@param connection_details DatabricksConnectionDetails
---@return string
local function fetch_oauth_token(connection_details)
    log.info("Fetching new OAuth M2M token for client id %q", connection_details.oauth_client_id)
    local body = http_client.request({
        url = connection_details.url .. "/oidc/v1/token",
        method = "POST",
        headers = {
            ["Content-Type"] = "application/x-www-form-urlencoded",
            Authorization = "Basic "
                    .. util.base64_encode(
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
            log.debug("Token already available, no need to fetch it again")
        end
        return token
    end
end

---@param token string
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
    local auth_mode = connection_details.auth
    if auth_mode == "token" then
        return create_bearer_token_provider(connection_details.token)
    end
    if auth_mode == "m2m" then
        return create_m2m_token_provider(connection_details)
    end
    local exa_error = tostring(ExaError:new("E-VSDAB-20", "Unsupported auth mode {{auth_mode}}.",
                                            {auth_mode = {value = auth_mode}}):add_ticket_mitigation())
    log.error(exa_error)
    error(exa_error)
end

return M
