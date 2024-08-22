require("busted.runner")()
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
---@return ConnectionDetails
local function read_connection(connection)
    return ConnectionReader:new(context_mock(connection)):read("my_connection")
end

local function read_address(address)
    return read_connection({address = address})
end

describe("ConnectionReader", function()

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
            {url = "jdbc:databricks://example.com:443", expected = {host = "example.com", port = 443}},
            {url = "jdbc:databricks://123.0.0.1:443", expected = {host = "123.0.0.1", port = 443}}, {
                url = "jdbc:databricks://abc-123def-456.cloud.databricks.com:443",
                expected = {host = "abc-123def-456.cloud.databricks.com", port = 443}
            }, {url = "jdbc:databricks://example.com:443;unknown=value", expected = {host = "example.com", port = 443}},
            {
                url = "jdbc:databricks://example.com:443;PWD=token",
                expected = {host = "example.com", port = 443, token = "token"}
            }
        }
        for _, test in ipairs(test_cases) do
            it("'" .. test.url .. "'", function()
                assert.are.same(test.expected, read_address(test.url))
            end)
        end
    end)
end)
