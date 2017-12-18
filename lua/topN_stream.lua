-- EVAL script 1 PQ_KEY userID n [seconds]


local topN = KEYS[1]
local user_history_key = '{' .. topN ..'}' .. ARGV[1]
local n = tonumber(ARGV[2])
local result = {}

if (redis.call('exists', topN) == 0) or (n < 1) then
	return result
end


-- TTL key
-- returns -2 if the key does not exist.
-- returns -1 if the key exists but has no associated expire.
local ttl = redis.call('TTL', user_history_key)
local ONE_DAY_IN_SECONDS = 86400

if  ttl == -2 then
	-- history is empty
	result = redis.call('ZREVRANGE', KEYS[1], 0, n - 1)
else
	-- ZREVRANGE key start stop [WITHSCORES]
	-- ZCARD key 
	local len = redis.call('ZCARD', topN)
	for i=0,(len/n - 1) do
		local tmp = redis.call('ZREVRANGE', KEYS[1], n * i, n*(i+1) - 1)
		for _,v in ipairs(tmp) do
			-- SISMEMBER key member
			if redis.call('SISMEMBER', user_history_key, v) == 0 then
				table.insert(result, v)
				if #result >= n then break end
			end
		end
		if #result >= n then break end
	end
end

if next(result) ~= nil then
	-- SADD key member [member ...] 
	local command = {'SADD', user_history_key}
	for _,v in ipairs(result) do
		table.insert(command, v)
	end
	redis.call(unpack(command))
	-- EXPIRE key seconds
	if ttl < 1 then
		if ARGV[3] ~= nil then
			ttl = tonumber(ARGV[3])
		else
			ttl = ONE_DAY_IN_SECONDS
		end
	end
	redis.call('EXPIRE', user_history_key, ttl)
end

return result