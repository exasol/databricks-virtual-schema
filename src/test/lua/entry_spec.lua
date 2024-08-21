require("busted.runner")()
require("entry")

describe("entry.adapter_call()", function()
    it("can call adapter function", function()
        _G.exa = {}
        adapter_call([[{"type":"createVirtualSchema"}]])
    end)
end)
