-- EVAL script 1 PQ_KEY capacity id new_score
-- return topN size

local topN = KEYS[1]
local capacity = tonumber(ARGV[1])
local id = ARGV[2]
local new_score = ARGV[3]

-- ZADD key score member
-- If key does not exist, a new sorted set is created
-- If a specified member is already a member of the sorted set, the score is updated
redis.call('ZADD', topN, new_score, id)

-- ZCARD key
local size = redis.call('ZCARD', topN)

if size > capacity then
	-- ZRANGE key start stop
	local min_id = redis.call('ZRANGE', topN, 0, 0)[1]
	-- ZREM key member
	redis.call('ZREM', topN, min_id)
	size = size - 1
end

return size