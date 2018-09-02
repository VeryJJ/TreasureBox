package com.huangshang.demo.jstorm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.client.ConfigExtension;
import com.huangshang.demo.jstorm.Utils.AbstractTopologyStarter;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpout;
import com.huangshang.demo.jstorm.kafka.plugin.KafkaSpoutConfig;
import com.huangshang.demo.jstorm.kafka.bolts.CustomBolt;
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
        builder.setBolt("customBolt", new CustomBolt(), 1).shuffleGrouping("kafkaSpout");

        return builder;
    }
}