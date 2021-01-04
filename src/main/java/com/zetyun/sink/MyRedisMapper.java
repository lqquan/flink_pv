package com.zetyun.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<Tuple2<String, Long>> {


    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET,null);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Long> stringLongTuple2) {
        return stringLongTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Long> stringLongTuple2) {
        return String.valueOf(stringLongTuple2.f1);
    }
}
