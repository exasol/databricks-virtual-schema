---@diagnostic disable: missing-fields, assign-type-mismatch # No need to specify unnecessary fields in test
require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local utils = require("exasol.adapter.databricks.test_utils")
local authentication = require("exasol.adapter.databricks.authentication")

log.set_level("INFO")

---@return DatabricksConnectionDetails
local function read_oauth_credentials()
    local config = utils.read_test_config()
    return {
        url = config["databricks.host"],
        auth = "m2m",
        oauth_client_id = config["databricks.oauth.clientId"],
        oauth_client_secret = config["databricks.oauth.secret"]
    }
end

local oauth_credentials = read_oauth_credentials()

describe("authentication", function()
    describe("create_token_provider", function()
        it("fails for missing auth mode", function()
            assert.has_error(function()
                authentication.create_token_provider({auth = nil})
            end, [[E-VSDAB-20: Unsupported auth mode <missing value>.

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]])
        end)
        it("fails for invalid auth mode", function()
            assert.has_error(function()
                authentication.create_token_provider({auth = "invalid"})
            end, [[E-VSDAB-20: Unsupported auth mode 'invalid'.

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]])
        end)
        describe("token auth #itest", function()
            it("returns bearer token", function()
                local provider = authentication.create_token_provider({token = "token", auth = "token"})
                assert.is.same("token", provider())
            end)
        end)
        describe("m2m auth #itest", function()
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
