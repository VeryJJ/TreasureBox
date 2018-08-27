package com.huangshang.demo.jstorm.case_redis;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by huangshang on 2018/8/25 下午9:28.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisWriteSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector spoutOutputCollector;

    /**
     * 作为字段word输出
     */
    private static final Map<Integer, String> LASTNAME = new HashMap<Integer, String>();
    static {
        LASTNAME.put(0, "anderson");
        LASTNAME.put(1, "watson");
        LASTNAME.put(2, "ponting");
        LASTNAME.put(3, "dravid");
        LASTNAME.put(4, "lara");
    }
    /**
     * 作为字段myValues输出
     */
    private static final Map<Integer, String> COMPANYNAME = new HashMap<Integer, String>();
    static {
        COMPANYNAME.put(0, "abc");
        COMPANYNAME.put(1, "dfg");
        COMPANYNAME.put(2, "pqr");
        COMPANYNAME.put(3, "ecd");
        COMPANYNAME.put(4, "awe");
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
        System.err.println("RedisWriteSpout#nextTuple#" + LASTNAME.get(randomNumber) + "->" + COMPANYNAME.get(randomNumber));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        spoutOutputCollector.emit (new Values(LASTNAME.get(randomNumber),COMPANYNAME.get(randomNumber)));
        System.out.println("写数据来袭！！！！！！");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // emit the field site.
        declarer.declare(new Fields("word","myValues"));
    }

}
