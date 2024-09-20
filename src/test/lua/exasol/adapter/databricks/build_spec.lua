require("busted.runner")()
local assert = require("luassert")
local mockagne = require("mockagne")
local DatabricksAdapter = require("exasol.adapter.databricks.DatabricksAdapter")

describe("Build setup", function()
    local function load_rockspec(path)
        local env = {}
        local rockspec_function = assert(loadfile(path, "t", env))
        rockspec_function()
        return env
    end

    local function get_current_version()
        local metadata_reader_mock = mockagne.getMock()
        return DatabricksAdapter:new(metadata_reader_mock):get_version()
    end

    local function get_rockspec_filename()
        return string.format("databricks-virtual-schema-%s-1.rockspec", get_current_version())
    end

    local function read_version_from_pom()
        local file = io.open("pom.xml", "r")
        assert(file, "Expected pom.xml to exist")
        finally(function()
            file:close()
        end)
        local pattern = "<version>(.*)</version>"
        for line in file:lines() do
            local version = string.match(line, pattern)
            if version then
                return version
            end
        end
        error("Version not found in pom.xml, expected pattern: " .. pattern)
    end

    describe("Rockspec file", function()
        it("has correct filename", function()
            local filename = get_rockspec_filename()
            local file = io.open(filename, "r")
            assert(file, "Expected rockspec " .. filename .. " to exist")
            finally(function()
                file:close()
            end)
            assert.is_not_nil(file, "Expected rockspec to have filename " .. filename)
        end)

        describe("version field", function()
            it("has type string", function()
                local rockspec = load_rockspec(get_rockspec_filename())
                assert.is_same("string", type(rockspec.version), "Rockspec version must be string")
            end)

            it("is equal to adapter version", function()
                local rockspec = load_rockspec(get_rockspec_filename())
                assert.is_same(get_current_version() .. "-1", rockspec.version,
                               "Rockspec version must be equal to version from DatabricksAdapter:get_version()")
            end)
        end)
    end)

    describe("pom.xml file", function()
        it("has same version as adapter", function()
            assert.is_same(get_current_version(), read_version_from_pom(),
                           "Version in pom.xml must be equal to version from DatabricksAdapter:get_version()")

        end)
    end)
end)
