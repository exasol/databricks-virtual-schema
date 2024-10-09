local http_client = require("exasol.adapter.databricks.http_client")
local base64 = require("exasol.adapter.databricks.base64")
local ExaError = require("ExaError")
local cjson = require("cjson")
local log = require("remotelog")

local M = {}

---@alias TokenProvider fun(): string

---@param user string?
---@param password string?
---@return string header_value
local function basic_auth_header(user, password)
    return "Basic " .. base64.encode(string.format("%s:%s", user, password))
end

---Fetch Databricks OAuth token.
---See https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html#manually-generate-and-use-access-tokens-for-oauth-m2m-authentication
---@param connection_details DatabricksConnectionDetails
---@return string
local function fetch_oauth_token(connection_details)
    local url = connection_details.url .. "/oidc/v1/token"
    log.trace("Fetching OAuth M2M token from %q", url)
    local body = http_client.request({
        url = url,
        method = "POST",
        headers = {
            ["Content-Type"] = "application/x-www-form-urlencoded",
            Authorization = basic_auth_header(connection_details.oauth_client_id, connection_details.oauth_client_secret)
        },
        request_body = "grant_type=client_credentials&scope=all-apis",
        verify_tls_certificate = false
    })
    ---@type DatabricksTokenResponse
    local data = cjson.decode(body)
    log.info("Received token of length %d of type %q with scope %q, expires in %d", #data.access_token,
              data.token_type, data.scope, data.expires_in)
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
    local auth_mode = connection_details.auth
    log.trace("Creating token provider for auth mode %q", auth_mode)
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
