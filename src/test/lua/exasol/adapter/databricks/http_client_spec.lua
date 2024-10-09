require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local http_client = require("exasol.adapter.databricks.http_client")
local cjson = require("cjson")
local utils = require("exasol.adapter.databricks.test_utils")

log.set_level("INFO")

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
    describe("_table_sink()", function()
        describe("sink", function()
            it("returns 0 when an error occurs", function()
                local sink, result_getter = http_client._table_sink()
                assert.is.same(0, sink("ignored chunk", "error message"))
            end)
            it("returns 1 when no error occurs", function()
                local sink, result_getter = http_client._table_sink()
                assert.is.same(1, sink("chunk", nil))
            end)
        end)
        describe("result_getter", function()
            it("returns empty string when nothing collected", function()
                local sink, result_getter = http_client._table_sink()
                assert.is.same("", result_getter())
            end)
            it("ignores chunk with error", function()
                local sink, result_getter = http_client._table_sink()
                sink("ignored chunk", "error message")
                assert.is.same("", result_getter())
            end)
            it("collects chunk when no error occurs", function()
                local sink, result_getter = http_client._table_sink()
                sink("chunk", nil)
                assert.is.same("chunk", result_getter())
            end)
            it("concatenates multiple chunks", function()
                local sink, result_getter = http_client._table_sink()
                sink("chunk1")
                sink("chunk2")
                sink("chunk3")
                assert.is.same("chunk1chunk2chunk3", result_getter())
            end)
        end)
    end)
end)
describe("_create_source()", function()
    it("returns nil for nil body", function()
        assert.is.same(nil, http_client._create_source(nil))
    end)
    it("source returns nil for empty body", function()
        local source = http_client._create_source("")
        assert(source ~= nil)
        assert.is.same(nil, source())
    end)
    it("source returns nil for repeated calls", function()
        local source = http_client._create_source("")
        assert(source ~= nil)
        assert.is.same(nil, source())
        assert.is.same(nil, source())
        assert.is.same(nil, source())
    end)
    it("returns source for body", function()
        local source = http_client._create_source("body")
        assert(source ~= nil)
        assert.is.same("body", source())
        assert.is.same(nil, source())
    end)
    it("ignores negative block size", function()
        local source = http_client._create_source("body", -1)
        assert(source ~= nil)
        assert.is.same("body", source())
        assert.is.same(nil, source())
    end)
    it("ignores zero block size", function()
        local source = http_client._create_source("body", 0)
        assert(source ~= nil)
        assert.is.same("body", source())
        assert.is.same(nil, source())
    end)
    it("splits body into chunks", function()
        local source = http_client._create_source("body", 2)
        assert(source ~= nil)
        assert.is.same("bo", source())
        assert.is.same("dy", source())
        assert.is.same(nil, source())
    end)
end)
describe("http_client #itest", function()
    describe("request()", function()
        it("fails for unknown host", function()
            assert.has_error(function()
                http_client.request({url = "https://unknown-host.example"})
            end, "E-VSDAB-6: HTTP request 'GET' for URL 'https://unknown-host.example' failed with result "
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
