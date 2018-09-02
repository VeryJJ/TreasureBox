package com.huangshang.demo.jstorm.kafka.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.huangshang.demo.jstorm.kafka.util.MySQLUtil;

import java.sql.SQLException;
import java.util.Date;

/**
 * Created by huangshang on 2018/9/2 下午1:19.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class MySQLBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -3741479683973598459L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String msgContent = input.getStringByField("kafka-data");

        System.out.println("MySQLBolt#execute   msgContent = " + msgContent);

        try {
            MySQLUtil.update(MySQLUtil.INSERT_LOG, msgContent, new Date());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.err.println("MySQLBolt#declareOutputFields");
    }
}
