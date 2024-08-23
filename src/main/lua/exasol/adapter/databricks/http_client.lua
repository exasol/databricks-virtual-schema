local log = require("remotelog")
local ExaError = require("ExaError")
local http = require("socket.http")

http.PROXY = nil
http.USERAGENT = "Exasol Databricks Virtual Schema"

---@class RequestArgs
---@field url string
---@field method "GET" | "POST" | nil
---@field headers table<string, string> | nil

local M = {}

local function table_sink()
    local result = {}
    local function sink(chunk, err)
        if err then
            log.error("Error while receiving response: %s", err)
            return 0
        end
        if chunk then
            log.trace("Received chunk #%d of size %d", #result, #chunk)
            table.insert(result, chunk)
        end
        return 1
    end
    local function result_getter()
        return table.concat(result, "\n")
    end
    return sink, result_getter
end

---@param args RequestArgs
---@return string response_body
function M.request(args)
    local url = args.url
    local method = args.method or "GET"
    local headers = args.headers or {}
    log.trace("Sending %s request to %s with %d headers", method, url, #headers)
    print(type(url))
    local sink, get_body = table_sink()
    local result, status_code, _response_headers, status_line = http.request({
        url = url,
        method = method,
        headers = headers,
        redirect = true,
        sink = sink
        -- create = create_tls_socket_factory()
    })
    if result ~= 1 then
        local exa_error = tostring(ExaError:new("E-VSDAB-6",
                                                "HTTP request for URL {{url}} failed with result {{result}}",
                                                {url = url, result = status_code}))
        log.error(exa_error)
        error(exa_error)
    end
    local body = get_body()
    if status_code ~= 200 then
        local exa_error = tostring(ExaError:new("E-VSDAB-5",
                                                "HTTP request for URL {{url}} failed with status {{status}} ({{status_message}}) and body {{body}}",
                                                {
            url = url,
            status = status_code,
            status_message = status_line,
            body = body
        }))
        log.error(exa_error)
        error(exa_error)
    end
    log.debug("Received response with status %d ('%s') and body size %d", status_code, status_line, #body)
    log.trace("Received body %s", body)
    return body
end

return M
