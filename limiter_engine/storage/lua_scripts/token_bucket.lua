-- KEYS[1]: The Redis key for this user/client (e.g., "ratelimit:user:123")
-- ARGV[1]: Max tokens capacity allowed in the bucket (int)
-- ARGV[2]: Refill rate (tokens per second) (float)
-- ARGV[3]: Current system Unix timestamp in seconds (int)
-- ARGV[4]: Requested tokens cost (usually 1 for a single API call)

local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- 1. Load the current bucket state from Redis
local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

-- 2. If this is a brand new user request, initialize a full bucket
if tokens == nil then
    tokens = max_tokens
    last_updated = now
else
    -- 3. Calculate how many tokens regenerated since the last request
    local elapsed = now - last_updated
    if elapsed > 0 then
        local generated = elapsed * refill_rate
        tokens = math.min(max_tokens, tokens + generated)
        last_updated = now
    end
end

-- 4. Evaluate allowance
local allowed = false
if tokens >= requested then
    tokens = tokens - requested
    allowed = true
end

-- 5. Calculate time remaining until the bucket is completely full again
local reset_time_seconds = now
if tokens < max_tokens then
    reset_time_seconds = math.ceil(now + ((max_tokens - tokens) / refill_rate))
end

-- 6. Save the new state back into the Redis Hash map and set an expiration time (TTL)
-- This automatically clears out idle users after 1 hour so your memory doesn't leak!
redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
redis.call("EXPIRE", key, 3600)

-- Return the precise parameters Sameer's protobuf expects: [allowed, remaining, reset_time]
return { allowed and 1 or 0, math.floor(tokens), reset_time_seconds }