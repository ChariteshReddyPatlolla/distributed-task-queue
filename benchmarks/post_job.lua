-- wrk POST /v1/jobs benchmark script
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.headers["X-API-Key"] = "bench-key-1"

local counter = 0

request = function()
    counter = counter + 1
    local body = string.format(
        '{"task_type":"cpu_hash","payload":{"iterations":500,"input":"benchmark-%d"},"priority":8,"max_retries":3}',
        counter
    )
    return wrk.format("POST", "/v1/jobs", wrk.headers, body)
end

response = function(status, headers, body)
    if status ~= 202 and status ~= 429 then
        -- Print unexpected error responses
        print(string.format("Status %d: %s", status, body))
    end
end
