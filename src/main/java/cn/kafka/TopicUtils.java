package cn.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangchen
 * @date 2018/8/14 16:01
 */
public class TopicUtils {

    private static final Properties kafkaProp = new Properties();

    public static void createTopic(String topicName) {
        ZkClient zkClient = new ZkClient("192.168.31.106:2181", 30000, 30000, ZKStringSerializer$.MODULE$);;
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        if (!AdminUtils.topicExists(zkUtils, topicName)) {
            AdminUtils.createTopic(zkUtils, topicName, 3, 2, kafkaProp, RackAwareMode.Enforced$.MODULE$);
        }
    }

    public static Properties selectTopic(String topicName) {
        ZkClient zkClient = new ZkClient("192.168.31.106:2181", 30000, 30000, ZKStringSerializer$.MODULE$);;
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        return AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topicName);
    }

    public static void main(String[] args){
        createTopic("avro");
    }
}
