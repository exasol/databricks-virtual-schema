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
                {url = "jdbc:databricks://example.com"}, --
                {url = "jdbc:databricks://example.com:"},
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
                {url = "arg=", properties = {arg = ""}}, --
                {url = "arg= ", properties = {arg = " "}}, --
                {url = "arg=;", properties = {arg = ""}}, --
                {url = "arg= ;", properties = {arg = " "}}, --
                {url = "arg=val", properties = {arg = "val"}}, --
                {url = "arg=Val", properties = {arg = "Val"}}, --
                {url = "arg=val-ue", properties = {arg = "val-ue"}}, --
                {url = "arg=val ue", properties = {arg = "val ue"}}, --
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
                {url = " arg1 = val1 ; arg2 = val2 ; ", properties = {arg1 = " val1 ", arg2 = " val2 "}}
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
        local tests = {
            {
                name = "missing connection",
                connection = nil,
                expected_error = "E-VSDAB-2: Connection 'my_connection' not found."
            }, {
                name = "missing address",
                connection = {address = nil},
                expected_error = "E-VSDAB-3: Connection 'my_connection' has no address."
            }, {
                name = "missing AuthMech",
                connection = {address = "jdbc:databricks://host:123"},
                expected_error = [[E-VSDAB-21: Connection 'my_connection' contains JDBC URL 'jdbc:databricks://host:123' without AuthMech property.

Mitigations:

* Specify one of the supported AuthMech values 3 (token auth) or 11 (M2M OAuth).]]
            }, {
                name = "unsupported AuthMech",
                connection = {address = "jdbc:databricks://host:123;AuthMech=unsupported"},
                expected_error = [[E-VSDAB-22: Connection 'my_connection' contains JDBC URL 'jdbc:databricks://host:123;AuthMech=unsupported' with unsupported AuthMech property value 'unsupported'.

Mitigations:

* Use one of the supported AuthMech values 3 (token auth) or 11 (M2M OAuth).]]
            }, {
                name = "unsupported numeric AuthMech",
                connection = {address = "jdbc:databricks://host:123;AuthMech=4"},
                expected_error = [[E-VSDAB-22: Connection 'my_connection' contains JDBC URL 'jdbc:databricks://host:123;AuthMech=4' with unsupported AuthMech property value '4'.

Mitigations:

* Use one of the supported AuthMech values 3 (token auth) or 11 (M2M OAuth).]]
            }, {
                name = "token auth: missing user",
                connection = {address = "jdbc:databricks://host:123;AuthMech=3"},
                expected_error = [[E-VSDAB-13: Connection 'my_connection' contains invalid user <missing value>.

Mitigations:

* Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.]]
            }, {
                name = "token auth: invalid user",
                connection = {address = "jdbc:databricks://host:123;AuthMech=3", user = "wrong_user"},
                expected_error = [[E-VSDAB-13: Connection 'my_connection' contains invalid user 'wrong_user'.

Mitigations:

* Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.]]
            }, {
                name = "token auth: missing password",
                connection = {address = "jdbc:databricks://host:123;AuthMech=3", user = "token", password = nil},
                expected_error = [[E-VSDAB-14: Connection 'my_connection' does not contain a valid token.

Mitigations:

* Please specify PASSWORD='<token>` in the connection.]]
            }, {
                name = "token auth: empty password",
                connection = {address = "jdbc:databricks://host:123;AuthMech=3", user = "token", password = ""},
                expected_error = [[E-VSDAB-14: Connection 'my_connection' does not contain a valid token.

Mitigations:

* Please specify PASSWORD='<token>` in the connection.]]
            }, {
                name = "m2m oauth: non-empty user",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11", user = "non-empty", password = nil},
                expected_error = [[E-VSDAB-23: Connection 'my_connection' uses M2M OAuth but 'USER' or 'IDENTIFIED BY' fields are not empty.

Mitigations:

* Use empty user and password or choose another authentication method.]]
            }, {
                name = "m2m oauth: non-empty password",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11", user = "", password = "non-empty"},
                expected_error = [[E-VSDAB-23: Connection 'my_connection' uses M2M OAuth but 'USER' or 'IDENTIFIED BY' fields are not empty.

Mitigations:

* Use empty user and password or choose another authentication method.]]
            }, {
                name = "m2m oauth: missing property Auth_Flow",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11"},
                expected_error = [[E-VSDAB-24: Connection 'my_connection' uses M2M OAuth but does not contain property 'Auth_Flow' or property has wrong value.

Mitigations:

* Specify property 'Auth_Flow=1' in JDBC URL.]]
            }, {
                name = "m2m oauth: wrong property value for Auth_Flow",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11;Auth_Flow=wrong"},
                expected_error = [[E-VSDAB-24: Connection 'my_connection' uses M2M OAuth but does not contain property 'Auth_Flow' or property has wrong value.

Mitigations:

* Specify property 'Auth_Flow=1' in JDBC URL.]]
            }, {
                name = "m2m oauth: missing OAuth2ClientId",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11;Auth_Flow=1"},
                expected_error = [[E-VSDAB-25: Connection 'my_connection' uses M2M OAuth but does not contain property 'OAuth2ClientId'.

Mitigations:

* Specify property 'OAuth2ClientId' in JDBC URL.]]
            }, {
                name = "m2m oauth: empty OAuth2ClientId",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11;Auth_Flow=1;OAuth2ClientId=;"},
                expected_error = [[E-VSDAB-25: Connection 'my_connection' uses M2M OAuth but does not contain property 'OAuth2ClientId'.

Mitigations:

* Specify property 'OAuth2ClientId' in JDBC URL.]]
            }, {
                name = "m2m oauth: empty OAuth2Secret",
                connection = {address = "jdbc:databricks://host:123;AuthMech=11;Auth_Flow=1;OAuth2ClientId=client;"},
                expected_error = [[E-VSDAB-26: Connection 'my_connection' uses M2M OAuth but does not contain property 'OAuth2Secret'.

Mitigations:

* Specify property 'OAuth2Secret' in JDBC URL.]]
            }, {
                name = "m2m oauth: empty OAuth2Secret",
                connection = {
                    address = "jdbc:databricks://host:123;AuthMech=11;Auth_Flow=1;OAuth2ClientId=client;OAuth2Secret=;"
                },
                expected_error = [[E-VSDAB-26: Connection 'my_connection' uses M2M OAuth but does not contain property 'OAuth2Secret'.

Mitigations:

* Specify property 'OAuth2Secret' in JDBC URL.]]
            }
        }
        for _, test in ipairs(tests) do
            it(test.name, function()
                assert.has_error(function()
                    read_connection(test.connection)
                end, test.expected_error)
            end)
        end

    end)

    describe("parsing invalid jdbc url fails for", function()
        local test_cases = {{url = ""}, {url = "jdbc:databricks://example.com"}, {url = "jdbc:exa://123.0.0.1:443"}}
        for _, test in ipairs(test_cases) do
            it(string.format("JDBC URL %q", test.url), function()
                assert.error_matches(function()
                    read_address(test.url)
                end, "E%-VSDAB%-4: Connection 'my_connection' contains invalid JDBC URL '" .. test.url .. "'")
            end)
        end
    end)
    describe("parsing valid jdbc url succeeds", function()
        describe("token auth", function()
            local test_cases = {
                {
                    url = "jdbc:databricks://abc-123def-456.cloud.databricks.com:443;AuthMech=3",
                    user = "token",
                    password = "myToken",
                    expected = {
                        url = "https://abc-123def-456.cloud.databricks.com:443",
                        auth = "token",
                        token = "myToken"
                    }
                }, {
                    url = "jdbc:databricks://host:443;PWD=ignored;AuthMech=3",
                    user = "token",
                    password = "myToken",
                    expected = {url = "https://host:443", auth = "token", token = "myToken"}
                }
            }
            for _, test in ipairs(test_cases) do
                it(string.format("JDBC URL %q", test.url), function()
                    assert.are.same(test.expected, read_address(test.url, test.user, test.password))
                end)
            end
        end)

        describe("m2m oauth", function()
            local test_cases = {
                {
                    url = "jdbc:databricks://abc-123def-456.cloud.databricks.com:443;AuthMech=11;Auth_Flow=1;OAuth2ClientId=client_id;OAuth2Secret=client_secret",
                    expected = {
                        url = "https://abc-123def-456.cloud.databricks.com:443",
                        auth = "m2m",
                        oauth_client_id = "client_id",
                        oauth_client_secret = "client_secret"
                    }
                }
            }
            for _, test in ipairs(test_cases) do
                it(string.format("JDBC URL %q", test.url), function()
                    assert.are.same(test.expected, read_address(test.url, test.user, test.password))
                end)
            end
        end)
    end)
end)
