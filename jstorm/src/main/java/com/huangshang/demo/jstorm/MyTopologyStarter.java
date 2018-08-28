package com.huangshang.demo.jstorm;

import com.huangshang.demo.jstorm.Utils.TopologyStarter;

import java.util.ServiceLoader;

/**
 * Created by huangshang on 2018/8/27 下午8:36.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class MyTopologyStarter {
    private static Boolean isLocalMode = Boolean.FALSE;

    public static void main(String[] args) throws Exception {
        ServiceLoader<TopologyStarter> topologyStarterList = ServiceLoader.load(TopologyStarter.class);

        int i = 1;
        for (TopologyStarter topology : topologyStarterList) {


            topology.submitTopology(isLocalMode);

            System.err.printf("提交第 %d 个 Topology 完毕。", i++);
        }

        System.err.println("共提交 " + i + " 个 Topology！");
    }

}
