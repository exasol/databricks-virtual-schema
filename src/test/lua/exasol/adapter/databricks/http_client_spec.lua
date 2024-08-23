require("busted.runner")()
local log = require("remotelog")
local http_client = require("exasol.adapter.databricks.http_client")
local cjson = require("cjson")

log.set_level("TRACE")

describe("http_client #itest", function()
    describe("request()", function()
        it("fails for unknown host", function()
            assert.has_error(function()
                http_client.request({url = "http://unknown-host.example"})
            end,
                             "E-VSDAB-6: HTTP request for URL 'http://unknown-host.example' failed with result 'host or service not provided, or not known'")
        end)

        it("sends unencrypted GET request", function()
            local response = http_client.request({
                url = "http://httpbin.org/get",
                headers = {Authentication = "Bearer token"}
            })
            response = cjson.decode(response)
            assert.is.same("http://httpbin.org/get", response.url)
            assert.is.same("Exasol Databricks Virtual Schema", response.headers["User-Agent"])
            assert.is.same("Bearer token", response.headers["Authentication"])
        end)

        it("sends TLS encrypted GET request", function()
            local response = http_client.request({
                url = "https://httpbin.org/get",
                headers = {Authentication = "Bearer token"}
            })
            response = cjson.decode(response)
            assert.is.same("https://httpbin.org/get", response.url)
            assert.is.same("Exasol Databricks Virtual Schema", response.headers["User-Agent"])
            assert.is.same("Bearer token", response.headers["Authentication"])
        end)
    end)

end)
