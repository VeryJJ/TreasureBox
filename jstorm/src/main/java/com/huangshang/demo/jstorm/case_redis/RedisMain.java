package com.huangshang.demo.jstorm.case_redis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by huangshang on 2018/8/25 下午9:21.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class RedisMain {
    private static Boolean isLocalMode = Boolean.FALSE;

    public static void main(String[] args) throws Exception {
//        if ((args != null) && (args.length > 0)){
//            isLocalMode = Boolean.FALSE;
//        }

//		writeRedis();
        readRedis();
    }
    /**
     * 写redis
     */
    public static void writeRedis(){
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();
        System.out.println("连接成功！！！");

        RedisStoreMapper storeMapper = new RedisWriteMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RedisWriteSpout", new RedisWriteSpout(), 2);
        builder.setBolt("to-save", storeBolt, 1).shuffleGrouping("RedisWriteSpout");

        Config conf = new Config();
        conf.put(Config.STORM_META_SERIALIZATION_DELEGATE, "org.apache.storm.serialization.SerializationDelegate");
//        conf.put(Config.STORM_META_SERIALIZATION_DELEGATE, "backtype.storm.serialization.DefaultSerializationDelegate");

        System.err.println("写入完成!!!!!");
        submit("redis-write-test", conf, builder);

    }
    /**
     * 读redis
     */
    public static void readRedis(){
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379).build();

        RedisLookupMapper lookupMapper = new RedisReadMapper();
        RedisLookupBolt lookupBolt = new RedisLookupBolt(poolConfig, lookupMapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RedisReadSpout-reader", new RedisReadSpout(), 2);
        builder.setBolt("to-lookupBolt", lookupBolt, 1).shuffleGrouping("RedisReadSpout-reader");
        builder.setBolt("to-out",new RedisOutBolt(), 1).shuffleGrouping("to-lookupBolt");

        Config conf = new Config();
        conf.put(Config.STORM_META_SERIALIZATION_DELEGATE, "org.apache.storm.serialization.SerializationDelegate");

        System.err.println("提交读topology!!!!!");
        submit("redis-read-test", conf, builder);

    }

    private static void submit(String topologyName, Config config, TopologyBuilder builder){
        if (isLocalMode) { // 本地运行, 可以看到 log 输出, 用于调试
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(topologyName, config, builder.createTopology());
            try {
                Thread.sleep(30000); // 30秒后自动停止 Topology
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                cluster.shutdown();
            }
        } else { // 集群运行
            try {
                StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }

}
