require("busted.runner")()
require("entry")

describe("entry.adapter_call()", function()
    it("can call adapter function", function()
        _G.exa = {
            get_connection = function(connection_name)
                return {address = "jdbc:databricks://localhost:8888;CWD=token"}
            end
        }
        adapter_call(
                [[{"type":"createVirtualSchema","schemaMetadataInfo":{"name":"new vs", "properties":{"CONNECTION_NAME":"my_connection"}}}]])
    end)
end)
