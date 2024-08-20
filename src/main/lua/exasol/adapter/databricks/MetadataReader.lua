--- This class reads schema, table and column metadata from the source.
-- @type MetadataReader
local MetadataReader = {}
MetadataReader.__index = MetadataReader

local log = require("remotelog")
local text = require("exasol.vscl.text")
local ExaError = require("ExaError")

local DEFAULT_SRID<const> = 0

--- Create a new `MetadataReader`.
-- @param exasol_context handle to local database functions and status
-- @return metadata reader
function MetadataReader:new(exasol_context)
    assert(exasol_context ~= nil,
           "The metadata reader requires an Exasol context handle in order to read metadata from the database")
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function MetadataReader:_init(exasol_context)
    self._exasol_context = exasol_context
end

--- Read the database metadata of the given schema (i.e. the internal structure of that schema)
-- <p>
-- The scan can optionally be limited to a set of user-defined tables. If the list of tables to include in the scan
-- is omitted, then all tables in the source schema are scanned and reported.
-- </p>
-- @return schema metadata
function MetadataReader:read(config)
    local tables = {}
    return {tables = tables, adapterNotes = "notes"}
end

return MetadataReader
