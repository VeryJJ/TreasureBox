package com.huangshang.demo.jstorm.Utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by huangshang on 2018/8/27 下午8:43.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public abstract class AbstractTopologyStarter implements TopologyStarter {
    @Override
    public void submitTopology(Boolean isLocalMode) {

        String topologyName = getTopologyName();
        Config config = getTopologyConfig();
        TopologyBuilder topologyBuilder = getTopologyBuilder();

        if (isLocalMode) { // 本地运行, 可以看到 log 输出, 用于调试

            new Thread(new Runnable() {
                @Override
                public void run() {
                    LocalCluster cluster = new LocalCluster();

                    cluster.submitTopology(topologyName, config, topologyBuilder.createTopology());

                    try {
                        Thread.sleep(30000); // 30秒后自动停止 Topology
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        cluster.shutdown();
                    }
                }
            }).start();


        } else { // 集群运行
            try {

                StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());

            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }

    protected abstract String getTopologyName();
    protected abstract Config getTopologyConfig();
    protected abstract TopologyBuilder getTopologyBuilder();

}
