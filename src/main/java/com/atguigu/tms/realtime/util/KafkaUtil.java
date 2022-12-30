package com.atguigu.tms.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
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

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;

public class KafkaUtil {

    private static final String DEFAULT_TOPIC = "default_topic";

    /**
     * 指定主题和消费者组获取 FlinkKafkaConsumer 对象
     *
     * @param topic   主题
     * @param groupId 消费者组
     * @param args    命令行参数数组
     * @return FlinkKafkaConsumer 实例
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId, String[] args) {
        Properties consumerProp = new Properties();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        topic = parameterTool.get("topic", topic);
        groupId = parameterTool.get("group-id", groupId);

        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null!");
        }

        if (groupId == null) {
            throw new IllegalArgumentException("GroupId cannot be null!");
        }

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
                        if (record != null && record.value() != null) {
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
     * 指定 topic 获取 FlinkKafkaProducer 实例
     *
     * @param topic 主题
     * @param args  命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic, String[] args) {
        // 创建配置对象
        Properties producerProp = new Properties();
        // 将命令行参数对象封装为 ParameterTool 类对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 提取命令行传入的 key 为 topic 的配置信息，并将默认值指定为方法参数 topic
        // 当命令行没有指定 topic 时，会采用默认值
        topic = parameterTool.get("topic", topic);
        // 如果命令行没有指定主题名称且默认值为 null 则抛出异常
        if (topic == null) {
            throw new IllegalArgumentException("主题名不可为空：命令行传参为空且没有默认值!");
        }

        // 获取命令行传入的 key 为 bootstrap-servers 的配置信息，并指定默认值
        String bootstrapServers = parameterTool.get(
                "bootstrap-severs", "hadoop102:9092, hadoop103:9092, hadoop104:9092");
        // 获取命令行传入的 key 为 transaction-timeout 的配置信息，并指定默认值
        String transactionTimeout = parameterTool.get(
                "transaction-timeout", 15 * 60 * 1000 + "");
        // 设置 Kafka 连接的 URL
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // 设置 Kafka 事务超时时间
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);

        // 内部类中使用但未声明的局部变量必须在内部类代码段之前明确分配
        String finalTopic = topic;
        return new FlinkKafkaProducer<String>(
                DEFAULT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(finalTopic, jsonStr.getBytes());
                    }
                },
                producerProp,
                EXACTLY_ONCE);
    }

    /**
     * 根据流中包含的目标主体信息，将数据打入 Kafka 的不同主题
     *
     * @param args 命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerBySchema(
            KafkaSerializationSchema<T> kafkaSerializationSchema, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties producerProp = new Properties();

        String bootstrapServers = parameterTool.get(
                "bootstrap-servers", "hadoop102:9092, hadoop103:9092, hadoop104:9092");
        String transactionTimeout = parameterTool.get(
                "transaction-timeout", 15 * 60 * 1000 + "");
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.common.serialization.kafka.ByteArraySerializer");
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.common.serialization.kafka.ByteArraySerializer");
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);

        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, producerProp, EXACTLY_ONCE);
    }

    public static String getUpsertKafkaDDL(String[] args) {

        return null;
    }
}
