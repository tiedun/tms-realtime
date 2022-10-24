package com.atguigu.tms.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class KafkaUtil {

    /**
     * 指定主题和消费者组获取 FlinkKafkaConsumer 对象
     * @param topic 主题
     * @param groupId 消费者组
     * @param args 命令行参数数组
     * @return FlinkKafkaConsumer 实例
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId, String[] args) {
        Properties consumerProp = new Properties();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String offsetReset = parameterTool.get("offset-reset", "latest");
        String bootstrapServers = parameterTool.get(
                "bootstrap-servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        consumerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                offsetReset);

        return new FlinkKafkaConsumer<String>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if(record != null && record.value() != null) {
                            return new String(record.value());
                        }
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                },
                consumerProp);
    }

    /**
     * 通过命令行参数获取主题和消费者组，获取 FlinkKafkaConsumer 实例
     * @param args 命令行参数数组
     * @return FlinkKafkaConsumer 实例
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String topic = parameterTool.get("topic", "default_topic");
        String groupId = parameterTool.get("group-id", "default_group");
        return getKafkaConsumer(topic, groupId, args);
    }

    /**
     * 指定 topic 获取 FlinkKafkaProducer 实例
     * @param topic 主题
     * @param args 命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic, String[] args) {
        Properties producerProp = new Properties();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String bootstrapServers = parameterTool.get(
                "bootstrap-severs", "hadoop102:9092, hadoop103:9092, hadoop104:9092");
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), producerProp);
    }

    /**
     * 通过命令行参数获取主题，获取 FlinkKafkaProducer 实例
     * @param args 命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String topic = parameterTool.get("topic", "default_topic");

        return getKafkaProducer(topic, args);
    }

    public static FlinkKafkaProducer<String> getKafkaProducerWithSchema(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties producerConfig = new Properties();

        String bootstrapServers = parameterTool.get(
                "bootstrap-servers", "hadoop102:9092, hadoop103:9092, hadoop104:9092");
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.common.serialization.kafka.StringSerializer");
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.common.serialization.kafka.StringSerializer");

        return new FlinkKafkaProducer<String>(
                "default_topic",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        JSONObject jsonObj = JSON.parseObject(element);
                        String topic = jsonObj.getString("topic");
                        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
                    }
                },
                producerConfig,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
