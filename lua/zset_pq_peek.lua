-- EVAL script 1 PQ_KEY 2 offset n [order]
-- order = [ASC | DESC] defalut DESC
-- retrun [(id, score)...] size <= n

-- ZCARD key
-- 0 if key does not exist.
local size = redis.call('ZCARD', KEYS[1])
local offset = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local order = ARGV[3]

if (offset < 0) or (n < 1) or (size < 1) then
	-- PQ is empty
	return {}
else
	if order == 'ASC' then
		-- ZRANGE key start stop [WITHSCORES] 
		return redis.call('ZRANGE', KEYS[1], offset, offset + (n - 1))
	else
		-- ZREVRANGE key start stop [WITHSCORES]
		return redis.call('ZREVRANGE', KEYS[1], offset,offset+(n - 1))
	end
end