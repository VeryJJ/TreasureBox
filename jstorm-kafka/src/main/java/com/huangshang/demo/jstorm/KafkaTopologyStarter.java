package com.huangshang.demo.jstorm;

import com.huangshang.demo.jstorm.kafka.MyKafkaTopology;

/**
 * Created by huangshang on 2018/8/27 下午8:36.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class KafkaTopologyStarter {
    private static Boolean isLocalMode = Boolean.FALSE;

    public static void main(String[] args) throws Exception {

        System.err.println("============= 开始提交 KafkaTopology ==============");

        MyKafkaTopology topology = new MyKafkaTopology();

        topology.submitTopology(isLocalMode);

        System.err.println("============= 完成提交 KafkaTopology ==============");

    }

}
