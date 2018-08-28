package com.huangshang.demo.jstorm.case_redis.write;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.huangshang.demo.jstorm.Utils.AbstractTopologyStarter;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

/**
 * Created by huangshang on 2018/8/25 下午9:21.
 * Description: 写redis demo
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisWriteTopology extends AbstractTopologyStarter{

    @Override
    protected String getTopologyName() {
        return "demo-redis-write-test";
    }

    @Override
    protected Config getTopologyConfig() {
        return new Config();
    }

    @Override
    protected TopologyBuilder getTopologyBuilder() {
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();
        System.out.println("连接成功！！！");

        RedisStoreMapper storeMapper = new RedisWriteMapper();
//        RedisStoreBolt storeBolt2 = new RedisStoreBolt(poolConfig, storeMapper);
        MyRedisStoreBolt storeBolt = new MyRedisStoreBolt(poolConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RedisWriteSpout", new RedisWriteSpout(), 2);
        builder.setBolt("to-save", storeBolt, 1).shuffleGrouping("RedisWriteSpout");

        System.err.println("Redis Write TopologyBuilder Init Success");

        return builder;
    }
}
