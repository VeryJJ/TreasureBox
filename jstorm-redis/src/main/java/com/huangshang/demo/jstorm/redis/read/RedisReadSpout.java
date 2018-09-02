package com.huangshang.demo.jstorm.redis.read;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by huangshang on 2018/8/25 下午9:25.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisReadSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector spoutOutputCollector;

    /**
     * 这是刚刚作为word写入的数据，要通过他获取我们存的值
     */
    private static final Map<Integer, String> LASTNAME = new HashMap<Integer, String>();
    static {
        LASTNAME.put(0, "anderson");
        LASTNAME.put(1, "watson");
        LASTNAME.put(2, "ponting");
        LASTNAME.put(3, "dravid");
        LASTNAME.put(4, "lara");
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        int randomNumber = rand.nextInt(5);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        spoutOutputCollector.emit (new Values(LASTNAME.get(randomNumber)));
        System.out.println("读数据来袭！！！！！！");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // emit the field site.
        declarer.declare(new Fields("word"));
    }

}
