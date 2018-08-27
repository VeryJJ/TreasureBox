package com.huangshang.demo.jstorm.case_redis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

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
        String strs =tuple.getString(1);
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
