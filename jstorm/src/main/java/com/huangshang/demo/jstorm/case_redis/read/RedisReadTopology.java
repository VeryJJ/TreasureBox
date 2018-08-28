package com.huangshang.demo.jstorm.case_redis.read;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.huangshang.demo.jstorm.Utils.AbstractTopologyStarter;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;

/**
 * Created by huangshang on 2018/8/25 下午9:21.
 * Description: 读Redis demo
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisReadTopology extends AbstractTopologyStarter{

    @Override
    protected String getTopologyName() {
        return "demo-redis-read-test";
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

        RedisLookupMapper lookupMapper = new RedisReadMapper();
//        RedisLookupBolt lookupBolt2 = new RedisLookupBolt(poolConfig, lookupMapper);
        MyRedisReadBolt lookupBolt = new MyRedisReadBolt(poolConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RedisReadSpout-reader", new RedisReadSpout(), 2);
        builder.setBolt("to-lookupBolt", lookupBolt, 1).shuffleGrouping("RedisReadSpout-reader");
        builder.setBolt("to-out",new RedisOutBolt(), 1).shuffleGrouping("to-lookupBolt");

        System.err.println("Redis Read TopologyBuilder Init Success");

        return builder;
    }
}
