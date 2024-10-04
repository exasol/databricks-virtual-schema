---@diagnostic disable: lowercase-global
-- LuaFormatter off
rockspec_format = "3.0"

local tag = "0.3.0"
local project = "databricks-virtual-schema"
local src = "src/main/lua"

package = project
version = tag .. "-1"

source = {url = "git://github.com/exasol/" .. project, tag = tag}

description = {
    summary = "Virtual Schema for connecting Databricks as a data source to Exasol",
    detailed = [[Virtual Schema for connecting Databricks as a data source to Exasol]],
    homepage = "https://github.com/exasol/" .. project,
    license = "MIT",
    maintainer = 'Exasol <opensource@exasol.com>'
}


dependencies = {
    "virtual-schema-common-lua = 5.0.0-1",
    "luasocket >= 3.1.0-1", -- Exasol uses 3.0rc1-2 but this causes test failures
    "luasec >= 1.0.2-1" -- Required for configuring TLS, same version as in Exasol
}

build_dependencies = {
    "amalg >= 0.8-1"
}

test_dependencies = {
    "busted >= 2.2.0-1",
    "luacheck >= 1.2.0-1",
    "luacov >= 0.15.0-1",
    "mockagne >= 1.0-2"
}

test = {
    type = "busted"
}

local package_items = {
    "exasol.adapter.databricks.adapter_capabilities",
    "exasol.adapter.databricks.DatabricksAdapter",
    "exasol.adapter.databricks.DatabricksAdapterProperties",
    "exasol.adapter.databricks.DatabricksQueryRewriter",
    "exasol.adapter.databricks.MetadataReader",
    "exasol.adapter.databricks.ConnectionReader",
    "exasol.adapter.databricks.TableAdapterNotes",
    "exasol.adapter.databricks.ColumnAdapterNotes",
    "exasol.adapter.databricks.PushdownMetadata",
    "exasol.adapter.databricks.DatabricksRestClient",
    "exasol.adapter.databricks.http_client",
    "exasol.adapter.databricks.util",
    "exasol.adapter.databricks.common_types",
    "exasol.adapter.databricks.databricks_types",
    -- from remotelog
    "remotelog",
    "ExaError",
    "MessageExpander",
    -- from virtual-schema-common-lua"
    "exasol.vscl.AbstractVirtualSchemaAdapter",
    "exasol.vscl.AdapterProperties",
    "exasol.vscl.RequestDispatcher",
    "exasol.vscl.Query",
    "exasol.vscl.QueryRenderer",
    "exasol.vscl.ImportQueryBuilder",
    "exasol.vscl.queryrenderer.AbstractQueryAppender",
    "exasol.vscl.queryrenderer.AggregateFunctionAppender",
    "exasol.vscl.queryrenderer.ExpressionAppender",
    "exasol.vscl.queryrenderer.ImportAppender",
    "exasol.vscl.queryrenderer.ScalarFunctionAppender",
    "exasol.vscl.queryrenderer.SelectAppender",
    "exasol.vscl.text",
    "exasol.vscl.validator",
    "exasol.vscl.types.type_definition",
}
-- LuaFormatter on

local item_path_list = ""
for i = 1, #package_items do
    item_path_list = item_path_list .. " " .. package_items[i]
end

build = {
    type = "command",
    build_command = "cd " .. src .. " && amalg.lua " .. "--output=../../../target/databricks-virtual-schema-dist-" .. tag
            .. ".lua " .. "--script=entry.lua" .. item_path_list
}
