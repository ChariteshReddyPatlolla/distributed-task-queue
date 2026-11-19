-- Atomic Token-Bucket Rate Limiter
-- KEYS[1]: ratelimit:{client_id}
-- ARGV[1]: capacity (number of maximum tokens)
-- ARGV[2]: refill_rate (tokens refilled per second)
-- ARGV[3]: current_timestamp (optional float in seconds; if absent, redis.call('TIME') is used)
-- ARGV[4]: cost (number of tokens requested, default 1)

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4] or 1)

if not now then
    local rtime = redis.call('TIME')
    now = tonumber(rtime[1]) + (tonumber(rtime[2]) / 1000000)
end

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens or not last_refill then
    tokens = capacity
    last_refill = now
else
    local elapsed = now - last_refill
    if elapsed > 0 then
        tokens = math.min(capacity, tokens + (elapsed * refill_rate))
        last_refill = now
    end
end

local allowed = 0
local retry_after = 0

if tokens >= cost then
    allowed = 1
    tokens = tokens - cost
else
    allowed = 0
    local needed = cost - tokens
    retry_after = math.max(1, math.ceil(needed / refill_rate))
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
redis.call('EXPIRE', key, 3600)

local missing = capacity - tokens
local reset_seconds = math.max(0, math.ceil(missing / refill_rate))
local remaining_tokens = math.max(0, math.floor(tokens))

return { allowed, remaining_tokens, reset_seconds, retry_after }
