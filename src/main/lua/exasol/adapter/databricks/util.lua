M = {}

---Convert all values in a table using a function.
---@generic T, U
---@param tbl table table to convert
---@param f fun(value: T): U mapper function to convert values
---@return table<T, U> converted table
function M.map(tbl, f)
    local t = {}
    for k, v in pairs(tbl) do
        t[k] = f(v)
    end
    return t
end

return M
