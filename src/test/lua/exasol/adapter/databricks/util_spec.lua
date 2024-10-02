require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local util = require("exasol.adapter.databricks.util")

describe("util", function()
    describe("base64_encode", function()
        local tests = {
            {data = nil, expected = ""}, --
            {data = "", expected = ""}, --
            {data = "a", expected = "YQ=="}, --
            {data = "ab", expected = "YWI="}, --
            {data = "abc", expected = "YWJj"}, --
            {data = "abcd", expected = "YWJjZA=="}, --
            {data = "Hello, World!", expected = "SGVsbG8sIFdvcmxkIQ=="},
            {data = "1234567890", expected = "MTIzNDU2Nzg5MA=="},
            {data = "Test with spaces", expected = "VGVzdCB3aXRoIHNwYWNlcw=="},
            {data = "Test\nwith\newlines", expected = "VGVzdAp3aXRoCmV3bGluZXM="},
            {data = "Test\twith\ttabs", expected = "VGVzdAl3aXRoCXRhYnM="},
            {data = "!@#$%^&*()", expected = "IUAjJCVeJiooKQ=="},
            {data = "öäüßÖÄÜ", expected = "w7bDpMO8w5/DlsOEw5w="}
        }
        for _, test in ipairs(tests) do
            it(string.format("encodes %q to %q", test.data, test.expected), function()
                assert.is.same(test.expected, util.base64_encode(test.data))
            end)
        end
    end)
end)
