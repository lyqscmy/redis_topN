import pytest
import redis
import time

# stream_script
# =============
# EVAL script 1 PQ_KEY userID n [seconds]

pq_key = 'test'
capacity = 10
ONE_DAY_IN_SECONDS = 86400


@pytest.fixture
def redis_conn():
    return redis.StrictRedis()


@pytest.fixture
def peek_script():
    with open("/home/liyongqiang/git/redis_pq/lua/zset_pq_peek.lua") as f:
        script = f.read()
    return script


@pytest.fixture
def add_script():
    with open("/home/liyongqiang/git/redis_pq/lua/zset_pq_add.lua") as f:
        script = f.read()
    return script


@pytest.fixture
def stream_script():
    with open("/home/liyongqiang/git/redis_pq/lua/topN_stream.lua") as f:
        script = f.read()
    return script


def test_next_empty(redis_conn, stream_script):
    # pq不存在或 n<1 时, 返回[]
    redis_conn.delete(pq_key)
    userID = 'id1'
    assert redis_conn.eval(stream_script, 1, pq_key, userID, -1) == []
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 0) == []
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 1) == []


def setup_topN(redis_conn, add_script, peek_script, stream_script):
    redis_conn.delete(pq_key)

    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id1', 1) == 1
    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id2', 2) == 2
    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id3', 3) == 3
    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id4', 4) == 4
    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id5', 5) == 5
    assert redis_conn.eval(add_script, 1, pq_key, capacity, 'id6', 6) == 6
    assert redis_conn.eval(peek_script, 1, pq_key, 0, capacity) == [
        b'id6', b'id5', b'id4', b'id3', b'id2', b'id1']


def test_next_one_user(redis_conn, add_script, peek_script, stream_script):
    setup_topN(redis_conn, add_script, peek_script, stream_script)

    userID = 'userid1'
    # 清空用户历史
    redis_conn.delete("{"+pq_key+"}"+userID)
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 3) == [
        b'id6', b'id5', b'id4']
    assert redis_conn.eval(stream_script, 1, pq_key,
                           userID, 2) == [b'id3', b'id2']
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 1) == [b'id1']
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 3) == []


def test_next_two_user(redis_conn, add_script, peek_script, stream_script):
    setup_topN(redis_conn, add_script, peek_script, stream_script)
    # 用户id不同next得到的stream不同
    test_next_one_user(redis_conn, add_script, peek_script, stream_script)

    # user_2与user_1的stream独立
    userID = 'userid2'
    # 清空user2的历史
    redis_conn.delete("{"+pq_key+"}"+userID)
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 3) == [
        b'id6', b'id5', b'id4']
    assert redis_conn.eval(stream_script, 1, pq_key,
                           userID, 2) == [b'id3', b'id2']
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 1) == [b'id1']
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 3) == []


def test_history_ttl(redis_conn, add_script, peek_script, stream_script):
    setup_topN(redis_conn, add_script, peek_script, stream_script)

    userID = 'userid1'
    n = 3
    # 清空用户历史
    user_history_key = "{"+pq_key+"}"+userID
    redis_conn.delete(user_history_key)
    ttl = 10

    assert redis_conn.eval(stream_script, 1, pq_key, userID, n, ttl) == [
        b'id6', b'id5', b'id4']
    ttl = redis_conn.ttl(user_history_key)
    assert ttl <= 10 and ttl > 0

    # 历史增加之后ttl延续
    assert redis_conn.eval(stream_script, 1, pq_key, userID, 2, ttl) == [
        b'id3', b'id2']
    time.sleep(3)
    ttl = redis_conn.ttl(user_history_key)
    assert ttl <= 7 and ttl > 0

    time.sleep(7)
    ttl = redis_conn.ttl(user_history_key)
    assert ttl == -2
