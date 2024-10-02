---@diagnostic disable: missing-fields # No need to specify unnecessary fields in test
require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local authentication = require("exasol.adapter.databricks.authentication")

---@return DatabricksConnectionDetails
local function read_oauth_credentials()
    local config = utils.read_test_config()
    return {
        url = config["databricks.host"],
        oauth_client_id = config["databricks.oauth.clientId"],
        oauth_client_secret = config["databricks.oauth.secret"]
    }
end

local oauth_credentials = read_oauth_credentials()

describe("authentication", function()
    describe("create_token_provider", function()
        it("fails when no credentials specified", function()
            assert.has_error(function()
                authentication.create_token_provider({})
            end, [[E-VSDAB-20: No Databricks credentials found.

Mitigations:

* Specify token or OAuth M2 credentials as specified in the user guide.]])
        end)
        describe("token auth", function()
            it("returns bearer token", function()
                local provider = authentication.create_token_provider({token = "token"})
                assert.is.same("token", provider())
            end)
        end)
        describe("m2m auth", function()
            it("fetches token", function()
                local provider = authentication.create_token_provider(oauth_credentials)
                local token = provider()
                assert.is.same(856, #token)
            end)
            it("caches token", function()
                local provider = authentication.create_token_provider(oauth_credentials)
                local token1 = provider()
                local token2 = provider()
                assert.is.same(token1, token2)
            end)
        end)
    end)
end)
