package com.huangshang.demo.jstorm.case_redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by huangshang on 2018/8/25 下午9:27.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisWriteMapper implements RedisStoreMapper {
    private static final long serialVersionUID = 1L;

    private RedisDataTypeDescription description;

    //这里的key是redis中的key
    private final String hashKey = "jstorm-test-write";

    public RedisWriteMapper() {
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public String getKeyFromTuple(ITuple ituple) {
        //这个代表redis中，hash中的字段名
        System.err.println("RedisWriteMapper#getKeyFromTuple#" + ituple.getStringByField("word"));
        return ituple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple ituple) {
        //这个代表redis中，hash中的字段名对应的值
        System.err.println("RedisWriteMapper#getValueFromTuple#" + ituple.getStringByField("myValues"));
        return ituple.getStringByField("myValues");
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }



}
