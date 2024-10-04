require("busted.runner")()
local assert = require("luassert")
local ConnectionReader = require("exasol.adapter.databricks.ConnectionReader")

---@param connection Connection?
---@return ExasolUdfContext
local function context_mock(connection)
    return {
        get_connection = function(self, name)
            return connection
        end
    }
end

---@param connection Connection?
---@return DatabricksConnectionDetails
local function read_connection(connection)
    return ConnectionReader:new(context_mock(connection)):read("my_connection")
end

local function read_address(address, user, password)
    return read_connection({address = address, user = user, password = password})
end

describe("ConnectionReader", function()
    describe("_parse_jdbc_url", function()
        describe("parses host and port", function()
            local tests = {
                {url = nil}, --
                {url = "invalid"}, --
                {url = "jdbc:databricks://"}, --
                {url = "jdbc:databricks://host", host = nil}, --
                {url = " jdbc:databricks://example.com:8080"}, --
                {url = " jdbc:exa://example.com:8080"}, --
                {url = "xjdbc:databricks://example.com:8080"}, --
                {url = "jdbc:databricks://example.com:8080 ", host = "example.com", port = 8080},
                {url = "jdbc:databricks://example.com:8080", host = "example.com", port = 8080},
                {url = "jdbc:databricks://127.0.0.1:8080", host = "127.0.0.1", port = 8080},
                {url = "jdbc:databricks://host:1", host = "host", port = 1}, --
                {url = "jdbc:databricks://host:443;", host = "host", port = 443}, --
                {url = "jdbc:databricks://host:443&arg=val", host = "host", port = 443, properties = {["&arg"] = "val"}},
                {
                    url = "jdbc:databricks://host:443;&arg=val",
                    host = "host",
                    port = 443,
                    properties = {["&arg"] = "val"}
                },
                {url = "jdbc:databricks://host:443-arg=val", host = "host", port = 443, properties = {["-arg"] = "val"}},
                {url = "jdbc:databricks://host:443;arg=val", host = "host", port = 443, properties = {arg = "val"}}
            }
            for _, test in ipairs(tests) do
                it(string.format("of JDBC url %q", test.url), function()
                    local host, port, properties = ConnectionReader._parse_jdbc_url(test.url)
                    assert.is.same(test.host, host)
                    assert.is.same(test.port, port)
                    if test.properties then
                        assert.is.same(test.properties, properties)
                    else
                        assert.is.same({}, properties)
                    end
                end)
            end
        end)

        describe("parses properties", function()
            local tests = {
                {url = "", properties = {}}, --
                {url = "invalid", properties = {}}, --
                {url = "123=val", properties = {["123"] = "val"}}, --
                {url = "arg=123", properties = {arg = "123"}}, --
                {url = "arg=val", properties = {arg = "val"}}, --
                {url = "arg=Val", properties = {arg = "Val"}}, --
                {url = "arg=val-ue", properties = {arg = "val-ue"}}, --
                {url = "Arg=val", properties = {Arg = "val"}}, --
                {url = "_arg_=val", properties = {_arg_ = "val"}}, --
                {url = "arg=val&", properties = {arg = "val&"}}, --
                {url = ";arg=val", properties = {arg = "val"}}, --
                {url = " arg=val", properties = {arg = "val"}}, --
                {url = "arg= val", properties = {arg = " val"}}, --
                {url = "arg =val", properties = {arg = "val"}}, --
                {url = "arg=val ", properties = {arg = "val "}}, --
                {url = "arg=val;", properties = {arg = "val"}}, --
                {url = "arg=val;;", properties = {arg = "val"}}, --
                {url = "arg=val; ", properties = {arg = "val"}}, --
                {url = "arg=val ;", properties = {arg = "val "}},
                {url = "arg1=val1;arg2=val2;", properties = {arg1 = "val1", arg2 = "val2"}},
                {url = "arg1=val1;;arg2=val2;", properties = {arg1 = "val1", arg2 = "val2"}},
                {url = "arg1=val1; arg2=val2;", properties = {arg1 = "val1", arg2 = "val2"}},
                {url = " arg1 = val1 ; arg2 = val2 ", properties = {arg1 = " val1 ", arg2 = " val2 "}}
            }
            for _, test in ipairs(tests) do
                it(string.format("of JDBC url %q", test.url), function()
                    local host, port, properties = ConnectionReader._parse_jdbc_url(string.format(
                            "jdbc:databricks://host:443;%s", test.url))
                    assert.is.same("host", host)
                    assert.is.same(443, port)
                    assert.is.same(test.properties, properties)
                end)
            end
        end)
    end)
    describe("handles invalid input", function()
        it("fails for missing connection", function()
            assert.has_error(function()
                read_connection(nil)
            end, "E-VSDAB-2: Connection 'my_connection' not found.")
        end)
        it("fails for missing address", function()
            assert.has_error(function()
                read_connection({})
            end, "E-VSDAB-3: Connection 'my_connection' has no address.")
        end)
        it("fails for missing user", function()
            assert.has_error(function()
                read_connection({address = "jdbc:databricks://example.com:123"})
            end, [[E-VSDAB-13: Connection 'my_connection' contains invalid user <missing value>.

Mitigations:

* Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.]])
        end)
        it("fails for any user that is not 'token'", function()
            assert.has_error(function()
                read_connection({address = "jdbc:databricks://example.com:123", user = "invalid"})
            end, [[E-VSDAB-13: Connection 'my_connection' contains invalid user 'invalid'.

Mitigations:

* Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.]])
        end)
        it("fails for missing password", function()
            assert.has_error(function()
                read_connection({address = "jdbc:databricks://example.com:123", user = "token"})
            end, [[E-VSDAB-14: Connection 'my_connection' does not contain a valid token.

Mitigations:

* Please specify PASSWORD='<token>` in the connection.]])
        end)
    end)

    describe("parsing invalid jdbc url fails for", function()
        local test_cases = {{url = ""}, {url = "jdbc:databricks://example.com"}, {url = "jdbc:exa://123.0.0.1:443"}}
        for _, test in ipairs(test_cases) do
            it("'" .. test.url .. "'", function()
                assert.error_matches(function()
                    read_address(test.url)
                end, "E%-VSDAB%-4: Connection 'my_connection' contains invalid JDBC URL '" .. test.url .. "'")
            end)
        end
    end)
    describe("parsing valid jdbc url succeeds for", function()
        local test_cases = {
            {
                url = "jdbc:databricks://example.com:8080",
                user = "token",
                password = "myToken",
                expected = {url = "https://example.com:8080", token = "myToken"}
            }, {
                url = "jdbc:databricks://123.0.0.1:443",
                user = "token",
                password = "myToken",
                expected = {url = "https://123.0.0.1:443", token = "myToken"}
            }, {
                url = "jdbc:databricks://abc-123def-456.cloud.databricks.com:443",
                user = "token",
                password = "myToken",
                expected = {url = "https://abc-123def-456.cloud.databricks.com:443", token = "myToken"}
            }, {
                url = "jdbc:databricks://example.com:443;unknown=value",
                user = "token",
                password = "myToken",
                expected = {url = "https://example.com:443", token = "myToken"}
            }, {
                url = "jdbc:databricks://example.com:443;PWD=token",
                user = "token",
                password = "myToken",
                expected = {url = "https://example.com:443", token = "myToken"}
            }
        }
        for _, test in ipairs(test_cases) do
            it("'" .. test.url .. "'", function()
                assert.are.same(test.expected, read_address(test.url, test.user, test.password))
            end)
        end
    end)
end)
