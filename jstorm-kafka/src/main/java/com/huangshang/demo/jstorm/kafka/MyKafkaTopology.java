package com.huangshang.demo.jstorm.kafka;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.huangshang.demo.jstorm.Utils.AbstractTopologyStarter;
import com.huangshang.demo.jstorm.kafka.bolts.KafkaMsgReadBolt;
import com.huangshang.demo.jstorm.kafka.bolts.MySQLBolt;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpout;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpoutConfig;
import com.huangshang.demo.jstorm.kafka.util.PropertiesUtil;

import java.util.Map;

public class MyKafkaTopology extends AbstractTopologyStarter{

    @Override
    protected String getTopologyName() {
        return "huangshang-kafka-topology";
    }

    @Override
    protected Config getTopologyConfig() {
        Config conf = new Config();

        conf.setDebug(false);
//        conf.setMaxTaskParallelism(3);
        return conf;
    }

    @Override
    protected TopologyBuilder getTopologyBuilder() {

        PropertiesUtil propertiesUtil = new PropertiesUtil("/application.properties", false);
        Map propsMap = propertiesUtil.getAllProperty();

        KafkaSpoutConfig spoutConfig = new KafkaSpoutConfig(propertiesUtil.getProps());
        spoutConfig.configure(propsMap);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("kafkaMsgReadBolt", new KafkaMsgReadBolt(), 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("MySQLStoreBolt", new MySQLBolt(), 1).shuffleGrouping("kafkaMsgReadBolt");

        return builder;
    }
}