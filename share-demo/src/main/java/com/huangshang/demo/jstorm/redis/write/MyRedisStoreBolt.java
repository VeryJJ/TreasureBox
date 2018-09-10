package com.huangshang.demo.jstorm.redis.write;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.huangshang.demo.jstorm.redis.constant.RedisKeySample;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import redis.clients.jedis.JedisCommands;

import java.util.Map;

/**
 * Created by huangshang on 2018/8/27 下午3:06.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class MyRedisStoreBolt extends BaseRichBolt {
    private static final long serialVersionUID = -8720810650765065115L;

    private JedisPoolConfig jedisPoolConfig;
    private transient JedisCommandsInstanceContainer container;
    protected OutputCollector collector;

    public MyRedisStoreBolt(){
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        //初始化Redis连接
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();

        this.jedisPoolConfig = poolConfig;
        if(this.jedisPoolConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(this.jedisPoolConfig);
        }
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField("kafka-data");
        JedisCommands jedisCommands = null;

        try {
            jedisCommands = this.container.getInstance();
            jedisCommands.hset(RedisKeySample.KEY_OF_CASE, key, "1");

            this.collector.ack(input);
        } catch (Exception var13) {
            this.collector.reportError(var13);
            this.collector.fail(input);
        } finally {
            this.container.returnInstance(jedisCommands);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
