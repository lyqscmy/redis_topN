import pytest
import redis
import time

# incrget_script
# ==============
# EVAL script 1 key incrment [seconds]
# seconds set the default ttl
# return key's new score

key = 'test'


@pytest.fixture
def redis_conn():
    return redis.StrictRedis()


@pytest.fixture
def incrget_script():
    with open("/home/liyongqiang/git/redis_pq/lua/incr_then_get.lua") as f:
        script = f.read()
    return script


def test_incrget_empty(redis_conn, incrget_script):

    redis_conn.delete(key)
    # 当key不存在时，创建key，初始值设为0, ttl为一天
    assert redis_conn.eval(incrget_script, 1, key, 1) == b'1'
    assert redis_conn.ttl(key) <= 86400


def test_incrget_incr(redis_conn, incrget_script):
    redis_conn.delete(key)
    assert redis_conn.eval(incrget_script, 1, key, 1) == b'1'

    time.sleep(10)
    # 当key存在时,增量incrment, ttl延续
    assert redis_conn.eval(incrget_script, 1, key, 2) == b'3'
    ttl = redis_conn.ttl(key)
    assert ttl > 0 and ttl <= 86390
