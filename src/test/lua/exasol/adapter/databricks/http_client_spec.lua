require("busted.runner")()
local log = require("remotelog")
local http_client = require("exasol.adapter.databricks.http_client")
local cjson = require("cjson")
local utils = require("exasol.adapter.databricks.test_utils")

log.set_level("TRACE")
local function read_databricks_test_config()
    local config = utils.read_test_config()
    return {url = config["databricks.host"], token = config["databricks.token"]}
end

local connection_details = read_databricks_test_config()

describe("http_client #itest", function()
    describe("request()", function()
        it("fails for unknown host", function()
            assert.has_error(function()
                http_client.request({url = "https://unknown-host.example"})
            end,
                             "E-VSDAB-6: HTTP request for URL 'https://unknown-host.example' failed with result 'host or service not provided, or not known'")
        end)

        it("unencrypted request not supported", function()
            assert.has_error(function()
                http_client.request({url = "http://example.com"})
            end, "E-VSDAB-7: Only HTTPS URLs are supported, but got 'http://example.com'")
        end)

        it("fails TLS certificate validation", function()
            assert.has_error(function()
                http_client.request({url = "https://example.com", verify_tls_certificate = true})
            end, "E-VSDAB-6: HTTP request for URL 'https://example.com' failed with result 'certificate verify failed'")
        end)

        it("sends TLS encrypted GET request", function()
            local response = http_client.request({
                url = "https://httpbin.org/get",
                headers = {Authentication = "Bearer token"},
                verify_tls_certificate = false
            })
            response = cjson.decode(response)
            assert.is.same("https://httpbin.org/get", response.url)
            assert.is.same("Exasol Databricks Virtual Schema", response.headers["User-Agent"])
            assert.is.same("Bearer token", response.headers["Authentication"])
        end)

        it("can connect to Databricks API", function()
            local response = http_client.request({
                url = connection_details.url .. "/api/2.1/unity-catalog/catalogs",
                headers = {Authentication = "Bearer " .. connection_details.token},
                verify_tls_certificate = false
            })
            response = cjson.decode(response)
            assert.is_true(#response.catalogs > 0)
        end)
    end)

end)
