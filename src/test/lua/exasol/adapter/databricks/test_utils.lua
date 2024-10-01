local assert = require("luassert")
local cjson = require("cjson")

local M = {}

---@param line string
---@return string? key, string? value
local function split_entry(line)
    local key, value = line:match("%s*([^=%s]+)%s*=%s*(.*)%s*")
    if key and key:match("#.*") then
        return nil, nil
    end
    return key, value
end

---Reads `test_config.json` file.
---@return table test_config content
function M.read_test_config()
    local entries = {}
    for line in io.lines("test.properties") do
        local key, value = split_entry(line)
        if key and value then
            entries[key] = value
        end
    end
    return entries
end

---@alias PredicateFunction fun(value: any): boolean

---Find the first element in a table that satisfies a predicate.
---@param tbl table table to search
---@param predicate PredicateFunction predicate to test elements
---@return any? element that satisfies the predicate
function M.find_first(tbl, predicate)
    for _, value in ipairs(tbl) do
        if predicate(value) then
            return value
        end
    end
    return nil
end

return M
