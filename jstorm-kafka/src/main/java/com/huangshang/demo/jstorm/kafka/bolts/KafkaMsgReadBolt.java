package com.huangshang.demo.jstorm.kafka.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import com.huangshang.demo.jstorm.kafka.util.ByteUtil;

import java.io.UnsupportedEncodingException;

/**
 * Created by huangshang on 2018/9/1 上午4:58.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class KafkaMsgReadBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1472482932572704961L;
//    protected final Logger logger = LoggerFactory.getLogger(KafkaMsgReadBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {

            String content = ByteUtil.getStringFromByteArray((byte[]) ((TupleImplExt) input).get("bytes"));
            System.out.println("KafkaMsgReadBolt#execute kafkaMsg = " + content);

            collector.emit(toTuple(content));

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("kafka-data"));
//        System.err.println("customBolt#declareOutputFields");
    }

    private Values toTuple(String msgContent){
        Values values = new Values();
        values.add(msgContent);

        return values;
    }
}
