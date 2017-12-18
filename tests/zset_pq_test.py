import pytest
import redis
import pathlib

# add_script
# ==========
# EVAL script 1 PQ_KEY capacity id new_score
# return topN size

# peek_script
# ===========
# EVAL script 1 PQ_KEY 2 offset n [order]
# order = [ASC | DESC] defalut DESC
# retrun [(id, score)...] size <= n

pq_key = 'test'
capacity = 3


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


def test_add_one(redis_conn, peek_script, add_script):
    # 当topN不存在时，add会创建topN，并加入(id, score)

    redis_conn.delete(pq_key)

    assert redis_conn.eval(peek_script, 1, pq_key, 0, capacity) == []

    idx = 'id1'
    score = 1
    assert redis_conn.eval(add_script, 1, pq_key, capacity, idx, score) == 1
    assert redis_conn.eval(peek_script, 1, pq_key, 0, capacity) == [b'id1']


def test_add_not_full(redis_conn, peek_script, add_script):
    test_add_one(redis_conn, peek_script, add_script)
    # 当topN存在时, 加入(id,score)

    # 容量未满，id不存在topN，则直接加入
    idx = 'id2'
    score = 2
    assert redis_conn.eval(add_script, 1, pq_key, capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id2', b'id1']

    # 容量未满，id存在topN，更新其score, 无论new_score大于或小于old_score
    score = 0
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id1', b'id2']

    score = 2
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id2', b'id1']


def test_add_full(redis_conn, peek_script, add_script):
    test_add_not_full(redis_conn, peek_script, add_script)
    # [id2, id1]
    # 当topN存在, 容量已满时, 加入(id,score)
    capacity = 2

    # id存在topN，则更新其score, 无论new_score大于或小于old_score
    idx = 'id2'
    score = 0
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id1', b'id2']

    score = 2
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id2', b'id1']

    # id不存在topN，new_score <= topN.min_score，则不加入
    idx = 'id3'
    score = 0
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id2', b'id1']

    # id不存在topN，new_score > topN.min_score，则删除掉topN.min_id, 添加（id, score）
    idx = 'id3'
    score = 3
    assert redis_conn.eval(add_script, 1, pq_key,
                           capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 10) == [b'id3', b'id2']


def test_peek_empty(redis_conn, peek_script):
    # topN不存在时, 永远返回[]
    pq_key = 'test:'
    redis_conn.delete(pq_key)
    # n < 1时默认返回[]，后续可能会修改为返回全部。
    assert redis_conn.eval(peek_script, 1, pq_key, 0, -1) == []
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 0) == []
    assert redis_conn.eval(peek_script, 1, pq_key, 0, 1) == []


def test_peek_ASC(redis_conn, peek_script, add_script):
    # 默认score从大到小peek, 添加可选参数可以从小到大peek
    pq_key = 'test:'
    n = 10
    redis_conn.delete(pq_key)

    capacity = 2
    idx = 'id1'
    score = 1
    assert redis_conn.eval(add_script, 1, pq_key, capacity, idx, score) == 1
    assert redis_conn.eval(peek_script, 1, pq_key, 0, n) == [b'id1']

    idx = 'id2'
    score = 2
    assert redis_conn.eval(add_script, 1, pq_key, capacity, idx, score) == 2
    assert redis_conn.eval(peek_script, 1, pq_key, 0, n) == [b'id2', b'id1']

    assert redis_conn.eval(peek_script, 1, pq_key, 0,
                           n, 'ASC') == [b'id1', b'id2']
    redis_conn.delete(pq_key)
