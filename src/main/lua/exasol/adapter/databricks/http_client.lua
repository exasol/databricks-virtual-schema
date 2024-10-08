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
---@field headers table<string, any> | nil
---@field request_body string?
---@field verify_tls_certificate boolean | nil default: true

local M = {}

---@alias ResponseBodySink fun(string, string): integer
---@alias ResponseBodyGetter fun(): string

---Creates an ltn12 sink for reading the response body as a string.
---Based on https://github.com/lunarmodules/luasocket/blob/master/src/ltn12.lua#L224
---@return ResponseBodySink, ResponseBodyGetter
---@private used in unit tests
function M._table_sink()
    local result = {}
    local function sink(chunk, err)
        if err then
            local exa_error =
                    ExaError:new("E-VSDAB-28", "Error while receiving HTTP response: {{error}}", {error = err})
            log.error(tostring(exa_error))
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

---An ltn12 source for sending request body.
---@alias BodySource fun(): string?

---Creates an ltn12 source for the given string data or `nil` if the data is nil.
---Based on https://github.com/lunarmodules/luasocket/blob/master/src/ltn12.lua#L118
---@param data string? optional body content
---@param block_size integer? optional block size, defaults to 2048
---@return BodySource? source the source or `nil` if the body is `nil`
---@private used in unit tests
function M._create_source(data, block_size)
    local DEFAULT_BLOCKSIZE<const> = 2048
    block_size = block_size or DEFAULT_BLOCKSIZE
    if data then
        local i = 1
        return function()
            local chunk = string.sub(data, i, i + block_size - 1)
            i = i + block_size
            if chunk ~= "" then
                log.trace("Sending request body until byte #%d: %q", i, chunk)
                return chunk
            else
                log.trace("No remaining data for body until byte #%d", i)
                return nil
            end
        end
    else
        log.trace("Send no request body")
        return nil
    end
end

---A factory for TCP sockets.
---@alias SocketFactory fun(args: table<string, any>): TCPSocket

---Create a new TCP socket factory configured with the given parameters.
---Adapted from https://stackoverflow.com/a/43067952
---@param params table socket parameters
---@return SocketFactory socket_factory new TCP socket factory
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

---Check if the given URL is unencrypted, i.e. starts with `http://`
---@param url string
---@return boolean is_unencrypted `true` if the URL is unencrypted
local function is_unencrypted(url)
    return url:match("^http://") ~= nil
end

---Create parameters for the TCP socket factory.
---@param verify_tls_certificate boolean `true` if the socket factory should verify the TLS certificate
---@return table<string, any> parameters socket factory parameters
local function get_socket_params(verify_tls_certificate)
    local verify_mode = verify_tls_certificate and "peer" or "none"
    return {protocol = "tlsv1_2", mode = "client", verify = verify_mode, options = "all"}
end

---Create a new custom TCP socket factory depending ono request arguments.
---@param args RequestArgs request arguments
---@return SocketFactory? socket_factory the custom socket factory or `nil` if the default socket factory should be used
---@private used in unit tests
function M._create_socket_factory(args)
    if is_unencrypted(args.url) then
        -- No custom socket factory required for unencrypted requests.
        return nil
    end
    local verify_tls_certificate = args.verify_tls_certificate == nil or args.verify_tls_certificate
    if verify_tls_certificate then
        -- TLS certificate should be verified. We can use the default socket factory.
        return nil
    else
        -- TLS certificate should be ignored. We need a custom socket factory.
        return new_socket_factory(get_socket_params(false))
    end
end

---Creates an ltn12 source for the given string data or `nil` if the data is nil.
---Based on https://github.com/lunarmodules/luasocket/blob/master/src/ltn12.lua#L118
---See details about ltn12: http://lua-users.org/wiki/FiltersSourcesAndSinks
---@param data string? body data content
---@return BodySource? source data iterator for body content blocks
local function create_source(data)
    local BLOCKSIZE<const> = 2048
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

---Execute an HTTP request with the given arguments
---@param args RequestArgs arguments for the HTTP request
---@return string response_body response body
function M.request(args)
    local url = args.url
    local method = args.method or "GET"
    local headers = args.headers or {}
    if args.request_body then
        headers["Content-Length"] = #args.request_body
    end
    log.trace("Sending %s request to %q", method, url)
    local sink, get_body = M._table_sink()
    local start_time = socket.gettime()
    local result, status_code, _response_headers, status_line = http.request({
        url = url,
        method = method,
        headers = headers,
        redirect = true,
        source = M._create_source(args.request_body),
        sink = sink,
        create = M._create_socket_factory(args)
    })
    if result ~= 1 then
        local exa_error = tostring(ExaError:new("E-VSDAB-6",
                                                "HTTP request {{method}} for URL {{url}} failed with result {{result}}",
                                                {method = method, url = url, result = status_code}))
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
