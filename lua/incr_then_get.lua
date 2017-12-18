-- EVAL script 1 key incrment [seconds]
-- seconds set the default ttl
-- return key's new score

local key = KEYS[1]
local incrment = ARGV[1]
local seconds = ARGV[2]

local ttl = redis.call('TTL', key)
redis.call('INCRBY', key, incrment)

if ttl < 0 then
	-- key not exists or no associated expire
	if seconds ~= nil then
		ttl = seconds
	else
		-- ONE_DAY_IN_SECONDS
		ttl = 86400
	end
end

redis.call('EXPIRE', key, ttl)
return redis.call('GET',key)