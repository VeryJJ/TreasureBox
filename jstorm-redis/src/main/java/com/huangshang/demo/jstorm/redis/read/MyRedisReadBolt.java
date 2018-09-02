package com.huangshang.demo.jstorm.redis.read;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.huangshang.demo.jstorm.redis.constant.RedisKeySample;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import redis.clients.jedis.JedisCommands;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by huangshang on 2018/8/27 下午10:26.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class MyRedisReadBolt extends BaseRichBolt {

    private static final long serialVersionUID = -3897822684235426575L;
    private final JedisPoolConfig jedisPoolConfig;

    private transient JedisCommandsInstanceContainer container;

    protected backtype.storm.task.OutputCollector collector;

    public MyRedisReadBolt(JedisPoolConfig jedisPoolConfig){
        this.jedisPoolConfig = jedisPoolConfig;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        if(this.jedisPoolConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(this.jedisPoolConfig);
        }
    }

    @Override
    public void execute(Tuple input) {

        System.err.println("MyRedisReadBolt#execute");

        String key = input.getStringByField("word");
        JedisCommands jedisCommand = null;

        try {
            jedisCommand = this.container.getInstance();
            Object lookupValue;

            lookupValue = jedisCommand.hget(RedisKeySample.KEY_OF_CASE, key);

            System.err.println("读到Redis数据 key = " + key + " value = " + (String)lookupValue);

            List<Values> values = toTuple(input, lookupValue);
            Iterator var6 = values.iterator();

            while(var6.hasNext()) {
                Values value = (Values)var6.next();
                this.collector.emit(input, value);
            }

            this.collector.ack(input);

        } catch (Exception var11) {
            this.collector.reportError(var11);
            this.collector.fail(input);
        } finally {
            this.container.returnInstance(jedisCommand);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //从redis中hash通过上面的key下面找到制定的word中的字段名下的值，有点想hbase中row：cf：val一样
        declarer.declare(new Fields("word","values"));
    }

    public List<Values> toTuple(ITuple input, Object value) {
        String member = input.getStringByField("word");

        List<Values> values = new ArrayList<>();

        //将拿到的数据存进集合,下面时将两个值返回的，所以向下游传值时需要定义两个名字。
        values.add(new Values(member,value));

        System.err.println("MyRedisReadBolt#toTuple#" + values.toString());

        return values;
    }
}
