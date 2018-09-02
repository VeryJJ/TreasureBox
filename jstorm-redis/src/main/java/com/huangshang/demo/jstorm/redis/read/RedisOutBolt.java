package com.huangshang.demo.jstorm.redis.read;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by huangshang on 2018/8/25 下午9:23.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisOutBolt extends BaseRichBolt {
    private static final long serialVersionUID = -3537799093128498062L;
    private OutputCollector collector;
    @Override
    public void execute(Tuple tuple) {
//				String str =tuple.getString(0);
        String strs = tuple.getString(1);
        System.err.println("RedisOutBolt#execute#" + strs);

    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector=collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("RedisOutBolt"));
    }

}
