require("busted.runner")()
local entry = require("entry")

describe("entry.adapter_call()", function()
    it("can call adapter function", function()
        _G.exa = {}
        adapter_call(
            [[{"type":"createVirtualSchema", "properties": {"SCHEMA_NAME":"schema", "TABLE_FILTER":"table1,table2"}}]])
    end)
end)
