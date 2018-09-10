package com.huangshang.demo.jstorm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.huangshang.demo.jstorm.Utils.AbstractTopologyStarter;
import com.huangshang.demo.jstorm.kafka.bolts.KafkaMsgReadBolt;
import com.huangshang.demo.jstorm.kafka.bolts.MsgKeyAnalyzeBolt;
import com.huangshang.demo.jstorm.kafka.bolts.MySQLPersistenceBolt;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpout;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpoutConfig;
import com.huangshang.demo.jstorm.kafka.util.PropertiesUtil;
import com.huangshang.demo.jstorm.redis.write.MyRedisStoreBolt;

import java.util.Map;

public class ShareDemoTopology extends AbstractTopologyStarter{

    @Override
    protected String getTopologyName() {
        return "zcy-share-demo-topology-huangshang";
    }

    @Override
    protected Config getTopologyConfig() {
        Config conf = new Config();

        conf.setDebug(false);
//        conf.setNumWorkers(2);
//        conf.setMaxTaskParallelism(3);
        return conf;
    }

    @Override
    protected TopologyBuilder getTopologyBuilder() {

        //读取Kafka Client参数
        PropertiesUtil propertiesUtil = new PropertiesUtil("/application.properties", false);
        Map propsMap = propertiesUtil.getAllProperty();

        //创建一个Kafka Spout作为数据源
        KafkaSpoutConfig spoutConfig = new KafkaSpoutConfig(propertiesUtil.getProps());
        spoutConfig.configure(propsMap);

        //编排Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("KafkaMsgReadBolt", new KafkaMsgReadBolt(), 1).shuffleGrouping("KafkaSpout");

        //Branch A
        //Kafka Msg 入MySQL
        builder.setBolt("MySQLPersistenceBolt", new MySQLPersistenceBolt(), 1).shuffleGrouping("KafkaMsgReadBolt");
        //带有关键字的Msg 进行Count, 入库。
        builder.setBolt("MsgKeyAnalyzeBolt", new MsgKeyAnalyzeBolt(), 1).shuffleGrouping("MySQLPersistenceBolt");

        //Branch B
        //Kafka Msg 入Redis
        builder.setBolt("RedisPersistenceBolt", new MyRedisStoreBolt(), 1).shuffleGrouping("KafkaMsgReadBolt");


        return builder;
    }


}