package cn.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author wangchen
 * @date 2018/8/12 14:16
 */
public class Producer implements Callback {

    private static final Properties kafkaProp = new Properties();

    private static final StringBuffer buffer = new StringBuffer();

    static {
        kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.106:9092,192.168.31.106:9093,192.168.31.106:9094");

        kafkaProp.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProp.put(ProducerConfig.RETRIES_CONFIG, "0");
        kafkaProp.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProp.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        kafkaProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        kafkaProp.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.31.106:8083");

        kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();

        buffer.append("{");
        buffer.append("       \"type\": \"record\",");
        buffer.append("       \"name\": \"Customer\",");
        buffer.append("       \"fields\": [");
        buffer.append("             {\"name\": \"customerID\", \"type\": [\"int\", \"null\"]},");
        buffer.append("             {\"name\": \"customerName\",  \"type\": \"string\"}");
        buffer.append("        ]");
        buffer.append("}");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProp);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(buffer.toString());

        /**
         * 没有进行复杂对象测试
         */
        GenericRecord customer = new GenericData.Record(schema);
        customer.put("customerID", 6);
        customer.put("customerName", "赵六");

        ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>("avro","key", customer);

        RecordMetadata recordMetadata = producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("消息发送失败！");
                    e.printStackTrace();
                } else {
                    System.out.println("消息发送成功！");
                }
            }
        }).get();//阻塞测试回调
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
    }
}
