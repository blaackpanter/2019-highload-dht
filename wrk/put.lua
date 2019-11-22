counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk.method = "PUT"
    counter = counter + 1
    wrk.body = "value" .. counter
    return wrk.format(nil, path)
end