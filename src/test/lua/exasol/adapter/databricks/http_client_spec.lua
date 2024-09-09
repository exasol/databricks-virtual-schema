require("busted.runner")()
local assert = require("luassert")
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

describe("http_client #utest", function()
    describe("_create_socket_factory()", function()
        local test_cases = {
            {url = "http://example.com", verify_tls_certificate = nil, expect_custom_socket_factory = false},
            {url = "http://example.com", verify_tls_certificate = true, expect_custom_socket_factory = false},
            {url = "http://example.com", verify_tls_certificate = false, expect_custom_socket_factory = false},
            {url = "https://example.com", verify_tls_certificate = nil, expect_custom_socket_factory = false},
            {url = "https://example.com", verify_tls_certificate = true, expect_custom_socket_factory = false},
            {url = "https://example.com", verify_tls_certificate = false, expect_custom_socket_factory = true}
        }
        for _, test in ipairs(test_cases) do
            it(string.format("URL='%s', verify_tls_certificate=%s", test.url, test.verify_tls_certificate), function()
                local actual_socket_factory = http_client._create_socket_factory({
                    url = test.url,
                    verify_tls_certificate = test.verify_tls_certificate
                })
                if test.expect_custom_socket_factory then
                    assert(actual_socket_factory ~= nil)
                    assert.is_function(actual_socket_factory)
                    local socket = actual_socket_factory({})
                    assert.is_table(socket)
                else
                    assert.is_nil(actual_socket_factory)
                end
            end)
        end
    end)
end)
describe("http_client #itest", function()
    describe("request()", function()
        it("fails for unknown host", function()
            assert.has_error(function()
                http_client.request({url = "https://unknown-host.example"})
            end, "E-VSDAB-6: HTTP request for URL 'https://unknown-host.example' failed with result "
                                     .. "'host or service not provided, or not known'")
        end)

        it("fails for non-200 status code", function()
            assert.error_matches(function()
                http_client.request({url = "https://example.com/invalidpath"})
            end, "E%-VSDAB%-5: HTTP request for URL 'https://example.com/invalidpath' failed with status 500")
        end)

        it("sends unencrypted GET request", function()
            local response = http_client.request({url = "http://example.com"})
            assert.is_true(response:match("<html>") ~= nil)
        end)

        it("sends encrypted GET request", function()
            local response = http_client.request({url = "https://example.com"})
            assert.is_true(response:match("<html>") ~= nil)
        end)

        it("can connect to Databricks API", function()
            local response = http_client.request({
                url = connection_details.url .. "/api/2.1/unity-catalog/catalogs",
                headers = {Authentication = "Bearer " .. connection_details.token},
                verify_tls_certificate = true
            })
            response = cjson.decode(response)
            assert.is_true(#response.catalogs > 0)
        end)
    end)

end)
