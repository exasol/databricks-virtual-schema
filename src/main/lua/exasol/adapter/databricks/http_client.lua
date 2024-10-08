local log = require("remotelog")
local ExaError = require("ExaError")
local http = require("socket.http")
local socket = require("socket")
local ssl = require("ssl")

http.PROXY = nil
http.USERAGENT = "Exasol Databricks Virtual Schema"

---@class RequestArgs
---@field url string
---@field method "GET" | "POST" | nil
---@field headers table<string, string> | nil
---@field request_body string?
---@field verify_tls_certificate boolean | nil default: true

---@alias SocketFactory fun(args: table<string, any>): TCPSocket

local M = {}

local function table_sink()
    local result = {}
    local function sink(chunk, err)
        if err then
            log.error("Error while receiving response: %s", err)
            return 0
        end
        if chunk then
            table.insert(result, chunk)
        end
        return 1
    end
    local function result_getter()
        return table.concat(result, "")
    end
    return sink, result_getter
end

---Create a new TCP socket factory configured with the given parameters.
---Adapted from https://stackoverflow.com/a/43067952
---@param params table
---@return SocketFactory socket_factory
local function new_socket_factory(params)
    return function()
        local t = {c = socket.try(socket.tcp())}
        function t:connect(host, port)
            ---@diagnostic disable-next-line: undefined-field
            socket.try(self.c:connect(host, port))
            self.c = socket.try(ssl.wrap(self.c, params))
            ---@diagnostic disable-next-line: undefined-field
            socket.try(self.c:dohandshake())
            return 1
        end
        return setmetatable(t, {
            -- Create proxy functions for each call through the metatable 
            __index = function(tbl, key)
                local f = function(prxy, ...)
                    local c = prxy.c
                    return c[key](c, ...)
                end
                tbl[key] = f -- Save new proxy function in cache for speed 
                return f
            end
        })
    end
end

---@param url string
---@return boolean is_unencrypted
local function is_unencrypted(url)
    return url:match("^http://") ~= nil
end

---@param verify_tls_certificate boolean
---@return table<string, any> args
local function get_socket_params(verify_tls_certificate)
    local verify_mode = verify_tls_certificate and "peer" or "none"
    return {protocol = "tlsv1_2", mode = "client", verify = verify_mode, options = "all"}
end

---@param args RequestArgs
---@return SocketFactory | nil socket_factory
---@private
function M._create_socket_factory(args)
    if is_unencrypted(args.url) then
        return nil
    end
    local verify_tls_certificate = args.verify_tls_certificate == nil or args.verify_tls_certificate
    if verify_tls_certificate then
        return nil
    else
        return new_socket_factory(get_socket_params(false))
    end
end

---@alias BodySource fun(): string?

---Creates an ltn12 source for the given string data or `nil` if the data is nil.
---Based on https://github.com/lunarmodules/luasocket/blob/master/src/ltn12.lua#L118
---@param data string? body data content
---@return BodySource? source data iterator for body content blocks
local function create_source(data)
    if data then
        local i = 1
        return function()
            local chunk = string.sub(data, i, i + BLOCKSIZE - 1)
            i = i + BLOCKSIZE
            if chunk ~= "" then
                return chunk
            else
                return nil
            end
        end
    else
        return nil
    end
end

---@param args RequestArgs
---@return string response_body
function M.request(args)
    local url = args.url
    local method = args.method or "GET"
    local headers = args.headers or {}

    log.trace("Sending %s request to %s with %d headers", method, url, #headers)
    local sink, get_body = table_sink()
    local start_time = socket.gettime()
    local result, status_code, _response_headers, status_line = http.request({
        url = url,
        method = method,
        headers = headers,
        redirect = true,
        source = create_source(args.request_body),
        sink = sink,
        create = M._create_socket_factory(args)
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
    local duration = math.floor((socket.gettime() - start_time) * 1000)
    log.debug("Received response with status %d ('%s') and body size %d in %dms", status_code, status_line, #body,
              duration)
    log.trace("Received body %s", body)
    return body
end

return M
