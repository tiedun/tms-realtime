package com.atguigu.tms.realtime.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtil {
    /**
     * 指定主题和消费者组获取 FlinkKafkaConsumer 对象
     *
     * @param topic   主题
     * @param groupId 消费者组
     * @param args    命令行参数数组
     * @return FlinkKafkaConsumer 实例
     */
    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId, String[] args) {
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
        consumerProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                offsetReset);

        return KafkaSource.<String>builder()
                .setTopics(topic)
                .setProperties(consumerProp)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null && bytes.length != 0) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }

    /**
     * 指定 topic 获取 FlinkKafkaProducer 实例
     *
     * @param topic 主题
     * @param args  命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static KafkaSink<String> getKafkaProducer(String topic, String[] args, String transId) {
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
        return KafkaSink.<String>builder()
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setKafkaProducerConfig(producerProp)
                // 如果指定语义为精准一次，那么每一个数据流向 kakfa 写入时，都会生成一个 transId。默认情况下 transId 的生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }

    public static KafkaSink<String> getKafkaProducer(String topic, String[] args) {
        return getKafkaProducer(topic, args, topic + "_trans");
    }

    /**
     * 根据流中包含的目标主体信息，将数据打入 Kafka 的不同主题
     *
     * @param args 命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static <T> KafkaSink<T> getKafkaProducerBySchema(
            KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema, String[] args, String transId) {
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

        return KafkaSink.<T>builder()
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .setKafkaProducerConfig(producerProp)
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果生产的精准一次性消费   那么每一个流数据在做向kakfa写入的时候  都会生成一个transId，默认名字生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }
}
