rockspec_format = "3.0"

local tag = "0.1.0"
local project = "databricks-virtual-schema"
local src = "src/main/lua"

package = project
version = tag .. "-1"

source = {
    url = "git://github.com/exasol/" .. project,
    tag = tag
}

description = {
    summary = "Virtual Schema for connecting Databricks as a data source to Exasol",
    detailed = [[Virtual Schema for connecting Databricks as a data source to Exasol]],
    homepage = "https://github.com/exasol/" .. project,
    license = "MIT",
    maintainer = 'Exasol <opensource@exasol.com>'
}

dependencies = {
    "virtual-schema-common-lua = 4.0.1-1"
}

build_dependencies = {
    "amalg >= 0.8-1"
}

test_dependencies = {
    "busted >= 2.2.0-1",
    "luacheck >= 1.2.0-1",
    "luacov >= 0.15.0-1"
}

test = {
    type = "busted"
}

local package_items = {
    "exasol.adapter.databricks.adapter_capabilities",
    "exasol.adapter.databricks.Adapter",
    "exasol.adapter.databricks.MetadataReader",
    "exasol.adapter.databricks.QueryRewriter",
    -- from remotelog
    "remotelog", "ExaError", "MessageExpander",
    -- from virtual-schema-common-lua"
    "exasol.vscl.AbstractVirtualSchemaAdapter",
    "exasol.vscl.AdapterProperties",
    "exasol.vscl.RequestDispatcher",
    "exasol.vscl.Query",
    "exasol.vscl.QueryRenderer",
    "exasol.vscl.queryrenderer.AbstractQueryAppender",
    "exasol.vscl.queryrenderer.AggregateFunctionAppender",
    "exasol.vscl.queryrenderer.ExpressionAppender",
    "exasol.vscl.queryrenderer.ImportAppender",
    "exasol.vscl.queryrenderer.ScalarFunctionAppender",
    "exasol.vscl.queryrenderer.SelectAppender",
    "exasol.vscl.text",
    "exasol.vscl.validator",
}

local item_path_list = ""
for i = 1, #package_items do
    item_path_list = item_path_list .. " " .. package_items[i]
end

build = {
    type = "command",
    build_command = "cd " .. src .. " && amalg.lua "
            .. "--output=../../../target/databricks-virtual-schema-dist-" .. tag .. ".lua "
            .. "--script=entry.lua"
            .. item_path_list
}
