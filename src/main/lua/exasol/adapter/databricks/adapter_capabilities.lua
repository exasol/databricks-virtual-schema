--- This module contains the list of supported adapter capabilities.
-- @module adapter_properties
M = {}

---Get capabilities supported by this adapter.
--- * Main capabilities: the following are **not** supported:
---   * `ORDER_BY_EXPRESSION`
---   * `JOIN_CONDITION_ALL`
---   * `AGGREGATE_HAVING`
---   * `AGGREGATE_GROUP_BY_EXPRESSION`
---   * `AGGREGATE_GROUP_BY_TUPLE`
--- * Literal capabilities: the following are **not** supported:
---   * `LITERAL_INTERVAL`
--- * Some `FN_*` capabilities are supported
---@return string[] supported_capabilities capabilities supported by the Databricks virtual schema
function M.get_capabilities()
    return {
        -- Main capabilities
        "SELECTLIST_PROJECTION", "SELECTLIST_EXPRESSIONS", "AGGREGATE_GROUP_BY_COLUMN", "AGGREGATE_SINGLE_GROUP",
        "FILTER_EXPRESSIONS", "JOIN", "JOIN_CONDITION_EQUI", "JOIN_TYPE_INNER", "JOIN_TYPE_FULL_OUTER",
        "JOIN_TYPE_LEFT_OUTER", "JOIN_TYPE_RIGHT_OUTER", "LIMIT", "LIMIT_WITH_OFFSET", "ORDER_BY_COLUMN", --
        -- Literal capabilities
        "LITERAL_BOOL", "LITERAL_DATE", "LITERAL_DOUBLE", "LITERAL_EXACTNUMERIC", "LITERAL_NULL", "LITERAL_STRING",
        "LITERAL_TIMESTAMP", "LITERAL_TIMESTAMP_UTC", --
        -- Predicate capabilities
        "FN_PRED_AND", "FN_PRED_OR", "FN_PRED_BETWEEN", "FN_PRED_EQUAL", "FN_PRED_NOTEQUAL", "FN_PRED_IN_CONSTLIST",
        "FN_PRED_IS_NOT_NULL", "FN_PRED_IS_NULL", "FN_PRED_LESS", "FN_PRED_LESSEQUAL", "FN_PRED_NOT", "FN_PRED_LIKE",
        "FN_PRED_LIKE_ESCAPE", --
        -- Conversion functions
        "FN_CAST", --
        -- Aggregate functions
        "FN_AGG_AVG", "FN_AGG_AVG_DISTINCT", "FN_AGG_COUNT", "FN_AGG_COUNT_DISTINCT", "FN_AGG_COUNT_STAR", "FN_AGG_MAX",
        "FN_AGG_MEDIAN", "FN_AGG_MIN", "FN_AGG_SUM", "FN_AGG_SUM_DISTINCT"
    }
end

return M
