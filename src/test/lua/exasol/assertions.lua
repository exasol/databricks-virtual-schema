local assert = require("luassert")
local say = require("say")
local cjson = require("cjson")

local M = {}

local function same_json(_, arguments)
    local expected<const> = arguments[1]
    local actual<const> = arguments[2]
    return assert.are.same(expected, cjson.decode(actual))
end

say:set("assertion.same_json.positive", "Expected %s\nto be a JSON encoded structure that matches: %s")
say:set("assertion.same_json.negative", "Expected %s\nto be a JSON encoded structure that differs from: %s")

assert:register("assertion", "same_json", same_json, "assertion.same_json.positive", "assertion.same_json.negative")

return M
