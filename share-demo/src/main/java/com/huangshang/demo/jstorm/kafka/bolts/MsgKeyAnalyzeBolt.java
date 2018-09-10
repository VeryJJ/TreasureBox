package com.huangshang.demo.jstorm.kafka.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import com.huangshang.demo.jstorm.kafka.util.ByteUtil;
import com.huangshang.demo.jstorm.kafka.util.MySQLUtil;
import com.huangshang.demo.jstorm.kafka.util.StringUtil;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Date;

public class MsgKeyAnalyzeBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1472482932572704961L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String msgContent = input.getStringByField("kafka-content");

        System.out.println("MsgKeyAnalyzeBolt#execute msgContent = " + msgContent);

        if (StringUtil.isEmpty(msgContent)){
            return;
        }

        try {

            //关键字消息做统计
            if (msgContent.contains("zcy")) {
                MySQLUtil.update(MySQLUtil.ADD_ZCY_COUNT);
            }

            if (msgContent.contains("error")) {
                MySQLUtil.update(MySQLUtil.ADD_ERROR_COUNT);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
