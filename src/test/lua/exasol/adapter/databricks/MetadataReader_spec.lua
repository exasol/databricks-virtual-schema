require("busted.runner")()
local assert = require("luassert")
local log = require("remotelog")
local MetadataReader = require("exasol.adapter.databricks.MetadataReader")
require("exasol.assertions")

log.set_level("INFO")

---@return ExasolUdfContext
local function context_mock()
    return {
        get_connection = function(self, name)
            return {address = "jdbc:databricks://host:443;PWD=token", user = "token", password = "myToken"}
        end
    }
end

---@param databricks_tables DatabricksTable[]
local function databricks_client_factory_mock(databricks_tables)
    return function()
        return {
            list_tables = function()
                return databricks_tables
            end
        }
    end
end

---@param databricks_tables DatabricksTable[]
---@return MetadataReader
local function testee(databricks_tables)
    return MetadataReader:new(context_mock(), databricks_client_factory_mock(databricks_tables))
end

---@param databricks_tables DatabricksTable[]
---@return ExasolSchemaMetadata
local function read_metadata(databricks_tables)
    local properties = {
        get_connection_name = function()
            return "connection"
        end,
        get_catalog_name = function()
            return "catalog"
        end,
        get_schema_name = function()
            return "schema"
        end
    }
    return testee(databricks_tables):read(properties)
end

---@param databricks_tables DatabricksTable[]
---@return ExasolTableMetadata[] tables
local function read_table_metadata(databricks_tables)
    return read_metadata(databricks_tables).tables
end

---@param databricks_type DatabricksType
---@return ExasolColumnMetadata column
local function map_data_type(databricks_type)
    local tables = read_table_metadata({
        {
            name = "table1",
            catalog_name = "catalog",
            schema_name = "schema",
            full_name = "schema.table1",
            table_type = "MANAGED",
            data_source_format = "DELTA",
            comment = "table comment",
            columns = {
                {
                    name = "col1",
                    comment = "col comment",
                    position = 1,
                    nullable = true,
                    type = databricks_type,
                    databricks_metadata = {org_col = "table1.col1"}
                }
            },
            databricks_metadata = {org_table = "table1"}
        }
    })
    assert.is.equal(1, #tables)
    assert.is.equal(1, #tables[1].columns)
    return tables[1].columns[1]
end

---@return ExasolTypeDefinition
---@param precision integer
---@param scale integer?
local function decimal_type(precision, scale)
    return {type = "DECIMAL", precision = precision, scale = scale or 0}
end

describe("MetadataReader", function()
    describe("read()", function()
        it("no table", function()
            local actual = read_table_metadata({})
            assert.is.same({}, actual)
        end)
        describe("maps data type", function()
            local tests = {
                {type_name = "STRING", expected = {type = "VARCHAR", size = 2000000}},
                {type_name = "BYTE", expected = decimal_type(3)}, --
                {type_name = "SHORT", expected = decimal_type(5)}, --
                {type_name = "INT", expected = decimal_type(10)}, --
                {type_name = "LONG", expected = decimal_type(19)}, --
                {type_name = "DECIMAL", type_text = "decimal(4,2)", expected = decimal_type(4, 2)}, --
                {type_name = "FLOAT", expected = {type = "DOUBLE"}}, --
                {type_name = "DOUBLE", expected = {type = "DOUBLE"}}, --
                {type_name = "BOOLEAN", expected = {type = "BOOLEAN"}}, --
                {type_name = "DATE", expected = {type = "DATE"}},
                {type_name = "TIMESTAMP", expected = {type = "TIMESTAMP", withLocalTimeZone = false}},
                {type_name = "TIMESTAMP_NTZ", expected = {type = "TIMESTAMP", withLocalTimeZone = false}}, --
                {
                    type_name = "INTERVAL",
                    type_text = "interval year",
                    expected = {type = "INTERVAL", fromTo = "YEAR TO MONTH", precision = 9}
                }
            }
            for _, test in ipairs(tests) do
                it(string.format("%s / %s", test.type_name, test.type_text), function()
                    local actual = map_data_type({name = test.type_name, text = test.type_text})
                    assert.is.same(test.expected, actual.dataType)
                    assert.is.same("col1", actual.name)
                    assert.is.same("col comment", actual.comment)
                    assert.is.same(nil, actual.default)
                    assert.is.same(true, actual.isNullable)
                end)
            end
        end)

        describe("maps char", function()
            local tests = {
                {type_text = nil, expected_length = 2000}, --
                {type_text = "unknown", expected_length = 2000}, --
                {type_text = "unknown(3)", expected_length = 2000}, --
                {type_text = "CHAR(4)", expected_length = 4}, --
                {type_text = "varchar(5)", expected_length = 5}, --
                {type_text = " char ( 7 ) ", expected_length = 7},
                {type_text = "\tchar\t(\t8\t)\t", expected_length = 8}, --
                {type_text = "char(0)", expected_length = 2000}, --
                {type_text = "char(-1)", expected_length = 2000}, -- 
                {type_text = "char(2000)", expected_length = 2000}, --
                {type_text = "char(2001)", expected_length = 2000}
            }
            for _, test in ipairs(tests) do
                it(string.format("%q", test.type_text), function()
                    local actual = map_data_type({name = "CHAR", text = test.type_text})
                    local expected = {type = "CHAR", size = test.expected_length}
                    assert.is.same(expected, actual.dataType)
                end)
            end
        end)

        describe("maps varchar", function()
            local tests = {
                {type_text = nil, expected_length = 2000000}, --
                {type_text = "unknown", expected_length = 2000000}, --
                {type_text = "unknown(3)", expected_length = 2000000}, --
                {type_text = "VARCHAR(4)", expected_length = 4}, --
                {type_text = "varchar(5)", expected_length = 5}, --
                {type_text = " varchar ( 7 ) ", expected_length = 7},
                {type_text = "\tvarchar\t(\t8\t)\t", expected_length = 8},
                {type_text = "varchar(0)", expected_length = 2000000},
                {type_text = "varchar(-1)", expected_length = 2000000},
                {type_text = "varchar(2000000)", expected_length = 2000000},
                {type_text = "varchar(2000001)", expected_length = 2000000}
            }
            for _, test in ipairs(tests) do
                it(string.format("%q", test.type_text), function()
                    local actual = map_data_type({name = "STRING", text = test.type_text})
                    local expected = {type = "VARCHAR", size = test.expected_length}
                    assert.is.same(expected, actual.dataType)
                end)
            end
        end)

        describe("maps decimal type text", function()
            local tests = {
                {type_text = "decimal(4,2)", expected_precision = 4, expected_scale = 2},
                {type_text = "DECIMAL(4,2)", expected_precision = 4, expected_scale = 2},
                {type_text = "decimal(2,5)", expected_precision = 2, expected_scale = 5},
                {type_text = " decimal ( 4 , 2 ) ", expected_precision = 4, expected_scale = 2},
                {type_text = "\tdecimal\t(\t4\t,\t2\t)\t", expected_precision = 4, expected_scale = 2},
                {type_text = "decimal(1,0)", expected_precision = 1, expected_scale = 0},
                {type_text = "decimal(36,0)", expected_precision = 36, expected_scale = 0},
                {type_text = "decimal(36,36)", expected_precision = 36, expected_scale = 36}
            }
            for _, test in ipairs(tests) do
                it(string.format("%q", test.type_text), function()
                    local actual = map_data_type({name = "DECIMAL", text = test.type_text})
                    local expected = decimal_type(test.expected_precision, test.expected_scale)
                    assert.is.same(expected, actual.dataType)
                end)
            end
        end)

        describe("maps interval types", function()
            local tests = {
                {type_text = "interval year", fromTo = "YEAR TO MONTH", fraction = nil},
                {type_text = "interval year to month", fromTo = "YEAR TO MONTH", fraction = nil},
                {type_text = "interval month", fromTo = "YEAR TO MONTH", fraction = nil},
                {type_text = "interval day", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval day to hour", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval day to minute", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval day to second", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval hour", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval hour to minute", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval hour to second", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval minute", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval minute to second", fromTo = "DAY TO SECONDS", fraction = 9},
                {type_text = "interval second", fromTo = "DAY TO SECONDS", fraction = 9}
            }
            for _, test in ipairs(tests) do
                it(string.format("%q", test.type_text), function()
                    local actual = map_data_type({name = "INTERVAL", text = test.type_text})
                    local expected = {type = "INTERVAL", fromTo = test.fromTo, precision = 9, fraction = test.fraction}
                    assert.is.same(expected, actual.dataType)
                end)
            end
        end)

        describe("raises error for invalid types", function()
            local tests = {
                {
                    name = "decimal precision > 36",
                    type_name = "DECIMAL",
                    type_text = "decimal(37,0)",
                    expected_error = [[E-VSDAB-11: Unsupported decimal precision 'decimal(37,0)' for column 'col1' at position 1 (comment: 'col comment'), Exasol supports a maximum precision of 36.

Mitigations:

* Please remove the column or change the data type.]]
                }, {
                    name = "missing decimal scale",
                    type_name = "DECIMAL",
                    type_text = "decimal(5)",
                    expected_error = [[E-VSDAB-10: Unknown Databricks decimal type 'decimal(5)' for column 'col1' at position 1 (comment: 'col comment')

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]]
                }, {
                    name = "missing decimal scale and precision",
                    type_name = "DECIMAL",
                    type_text = "decimal",
                    expected_error = [[E-VSDAB-10: Unknown Databricks decimal type 'decimal' for column 'col1' at position 1 (comment: 'col comment')

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]]
                }, {
                    name = "invalid interval type",
                    type_name = "INTERVAL",
                    type_text = "invalid",
                    expected_error = [[E-VSDAB-9: Unknown Databricks interval type 'invalid' for column 'col1' at position 1 (comment: 'col comment')

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]]
                }
            }
            for _, test in ipairs(tests) do
                it(test.name, function()
                    assert.has_error(function()
                        map_data_type({name = test.type_name, text = test.type_text})
                    end, test.expected_error)
                end)
            end
        end)

        describe("raises error for unsupported types", function()
            local tests = {"BINARY"}
            for _, type in ipairs(tests) do
                it(type, function()
                    assert.has_error(function()
                        map_data_type({name = type, text = ""})
                    end, string.format(
                            [[E-VSDAB-8: Exasol does not support Databricks data type '%s' of column 'col1' at position 1 with comment 'col comment'

Mitigations:

* Please remove the column or change the data type.]], type))
                end)
            end
        end)

        describe("maps generic types to VARCHAR", function()
            local tests = {"ARRAY", "MAP", "STRUCT", "VARIANT"}
            for _, type in ipairs(tests) do
                it(type, function()
                    local actual = map_data_type({name = type, text = ""})
                    local expected = {type = "VARCHAR", size = 2000000}
                    assert.is.same(expected, actual.dataType)
                end)
            end
        end)

        it("raises error for unknown type", function()
            assert.has_error(function()
                map_data_type({name = "unknown", text = "text", precision = 1, scale = 2})
            end,
                             [[E-VSDAB-7: Unknown Databricks data type 'unknown' / 'text' for column 'col1' at position 1 (precision: 1, scale: 2, comment: 'col comment')

Mitigations:

* This is an internal software error. Please report it via the project's ticket tracker.]])

        end)
        describe("adds original databricks metadata to adapter notes", function()
            ---@type DatabricksTable[]
            local databricks_tables = {
                {
                    catalog_name = "cat",
                    schema_name = "schema",
                    name = "name",
                    full_name = "fullname",
                    table_type = "type",
                    comment = "comment",
                    data_source_format = "format",
                    columns = {
                        {
                            name = "col",
                            nullable = true,
                            position = 0,
                            type = {name = "STRING", text = "typeText"},
                            comment = "comment",
                            databricks_metadata = {metadata = "for column"}
                        }
                    },
                    databricks_metadata = {metadata = "for table"}
                }
            }
            it("no metadata for vs adapter notes", function()
                local metadata = read_metadata(databricks_tables)
                assert.is.same(nil, metadata.adapterNotes)
            end)
            it("table adapter notes", function()
                local metadata = read_metadata(databricks_tables)
                assert.is.same_json({
                    catalog_name = "cat",
                    schema_name = "schema",
                    databricks_metadata = {metadata = "for table"}
                }, metadata.tables[1].adapterNotes)
            end)
            it("column adapter notes", function()
                local metadata = read_metadata(databricks_tables)
                assert.is.same_json({databricks_metadata = {metadata = "for column"}},
                                    metadata.tables[1].columns[1].adapterNotes)
            end)
        end)
    end)
end)
