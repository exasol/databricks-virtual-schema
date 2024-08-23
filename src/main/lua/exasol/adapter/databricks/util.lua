M = {}

---@alias MapperFunction fun(value: any): any

---Convert all values in a table using a function.
---@param tbl table table to convert
---@param f MapperFunction function to convert values
---@return table converted table
function M.map(tbl, f)
    local t = {}
    for k, v in pairs(tbl) do
        t[k] = f(v)
    end
    return t
end

return M
