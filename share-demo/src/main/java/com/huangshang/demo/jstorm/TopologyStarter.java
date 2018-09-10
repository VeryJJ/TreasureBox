package com.huangshang.demo.jstorm;

/**
 * Created by huangshang on 2018/8/27 下午8:36.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class TopologyStarter {
//    private static Boolean isLocalMode = Boolean.TRUE;
    private static Boolean isLocalMode = Boolean.FALSE;

    public static void main(String[] args) throws Exception {

        System.err.println("============= 开始提交 Share Demo Topology ==============");

        ShareDemoTopology topology = new ShareDemoTopology();

        topology.submitTopology(isLocalMode);

        System.err.println("============= 完成提交 Share Demo Topology ==============");

    }

}
