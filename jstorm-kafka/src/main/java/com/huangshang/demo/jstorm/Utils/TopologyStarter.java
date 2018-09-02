package com.huangshang.demo.jstorm.Utils;

/**
 * Created by huangshang on 2018/8/27 下午8:42.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public interface TopologyStarter {
    void submitTopology(Boolean isLocalMode);
}
