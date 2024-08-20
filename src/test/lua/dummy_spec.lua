require("busted.runner")()
local dummy = require("dummy")

describe("dummy", function()
    it("dummy", function()
        assert.is_same(2, 1 + 1)
        dummy.hello()
    end)
end)
