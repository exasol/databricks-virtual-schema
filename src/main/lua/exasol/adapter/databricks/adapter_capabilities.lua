--- This module contains the list of supported adapter capabilities.
-- @module adapter_properties
M = {}

---Get capabilities supported by this adapter.
---@return string[] supported_capabilities capabilities supported by the Databricks virtual schema
function M.get_capabilities()
    return {"SELECTLIST_PROJECTION", "SELECTLIST_EXPRESSIONS", "ORDER_BY_COLUMN"}
end

return M
