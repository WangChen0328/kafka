package cn.kafka;


import cn.pojo.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author wangchen
 * @date 2018/8/13 15:00
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private static final Properties kafkaProp = new Properties();

    private KafkaConsumer<String, GenericData.Record> consumer;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    static {
        kafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.106:9092,192.168.1.106:9093,192.168.1.106:9094");

        /**
         * kafka的config配置文件：consumer.properties文件里的group.id=test-consumer-group需要改成一致。
         */
        kafkaProp.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        kafkaProp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        kafkaProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProp.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        kafkaProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        kafkaProp.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.1.106:8083");

        kafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    }
    
    public void main(){
        consumer = new KafkaConsumer<>(kafkaProp);

        try {
            consumer.subscribe(Collections.singletonList("avro"), new HandlerRebalance());

            while(true) {
                ConsumerRecords<String, GenericData.Record> records = consumer.poll(1000);

                for (ConsumerRecord<String, GenericData.Record> record : records) {

                    /**
                     * 模拟消费信息
                     * 拟用Netty WebSocket 发送消息
                     */
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country= %s ",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );

                    GenericData.Record value = record.value();

                    ObjectMapper mapper = new ObjectMapper();
                    /**
                     * 没有对 value.toString() 经过复杂对象的测试
                     */
                    Customer customer = mapper.readValue(value.toString(), Customer.class);

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata")
                            );
                }

                consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                    /**
                     * 异步提交失败，记录数据库，另起线程，单独处理
                     * @param map
                     * @param e
                     */
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (WakeupException e) {

        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } catch (Exception e) {
                consumer.close();
            }
        }
    }

    private class HandlerRebalance implements ConsumerRebalanceListener {

        /**
         * 在平衡前和停止读取之后
         * @param partitions
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        }
    }
}
