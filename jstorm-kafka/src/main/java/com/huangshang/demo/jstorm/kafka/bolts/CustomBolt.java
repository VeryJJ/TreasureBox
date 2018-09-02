package com.huangshang.demo.jstorm.kafka.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import com.huangshang.demo.jstorm.kafka.util.ByteUtil;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * Created by huangshang on 2018/9/1 上午4:58.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class CustomBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1472482932572704961L;
//    protected final Logger logger = LoggerFactory.getLogger(CustomBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {

            String ss = ByteUtil.getStringFromByteArray((byte[]) ((TupleImplExt) input).get("bytes"));
            System.out.println("CustomBolt#execute   kafkaMsg = " + ss);
//            logger.info(ss);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.err.println("declareOutputFields");
    }
}
