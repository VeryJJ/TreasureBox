package com.huangshang.demo.jstorm.redis.read;

import com.google.common.collect.Lists;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by huangshang on 2018/8/25 下午9:24.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisReadMapper implements RedisLookupMapper {
    private static final long serialVersionUID = 1L;

    //对redis的所支持的种类进行了初始化
    private RedisDataTypeDescription description;

    //你想要读取的hash表中的key,这里使用的是刚刚存储的key字段名
    private final String hashKey = "jstorm-test";

    /**
     * redis中储存结构为hash hashKey为根key 然后在通过getKeyFromTuple 获得的key找到相对于的value
     * key1-key2[]-value  key2中的每一个key对应一个value
     * lookupValue = jedisCommand.hget(additionalKey, key);
     */
    public RedisReadMapper() {
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        //获取传过来的字段名
        System.err.println("RedisReadMapper#getKeyFromTuple#" + tuple.getStringByField("word"));
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return null;
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //从redis中hash通过上面的key下面找到制定的word中的字段名下的值，有点想hbase中row：cf：val一样
        declarer.declare(new Fields("word","values"));
    }

    @Override
    /**
     * 将拿到的数据装进集合并且返回
     */
    public List<Values> toTuple(ITuple input, Object value) {
        String member =getKeyFromTuple(input);

        List<Values> values = Lists.newArrayList();

        //将拿到的数据存进集合,下面时将两个值返回的，所以向下游传值时需要定义两个名字。
        values.add(new Values(member,value));
        System.err.println("RedisReadMapper#toTuple#" + values.toString());
        return values;
    }

}
