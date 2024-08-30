M = {}

---Convert all values in a table using a function.
---@generic K, T, U
---@param tbl table<K, T> table to convert
---@param f fun(value: T): U mapper function to convert values
---@return table<K, U> converted table
function M.map(tbl, f)
    local t = {}
    for k, v in pairs(tbl) do
        t[k] = f(v)
    end
    return t
end

---Filter values in a table using a predicate function.
---@generic K, T
---@param tbl table<K, T> table to filter
---@param predicate fun(key: K, value: T): boolean predicate function
---@return table<K, T> filtered table
function M.filter_table(tbl, predicate)
    local t = {}
    for k, v in pairs(tbl) do
        if predicate(k, v) then
            t[k] = v
        end
    end
    return t
end

---Filter values in an list using a predicate function.
---@generic T
---@param list T[] list to filter
---@param predicate fun(value: T): boolean predicate function
---@return  T[] filtered_list
function M.filter_list(list, predicate)
    local result = {}
    for _, v in pairs(list) do
        if predicate(v) then
            table.insert(result, v)
        end
    end
    return result
end

return M
