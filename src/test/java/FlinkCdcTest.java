import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by 铁盾 on 2022/10/24
 */
public class FlinkCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.disableOperatorChaining();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 10000);

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("tms")
                .tableList(".*")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> configSource =
                env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-source")
                        .setParallelism(4);

        configSource
                .print()
                .setParallelism(4);

        MapStateDescriptor<String, String> v = new MapStateDescriptor<>("12", String.class, String.class);

        KeyedStream<String, String> keyedStream = source1.keyBy(r -> "1");
        KeyedStream<String, String> keyedStream1 = configSource.keyBy(r -> "1");
        BroadcastStream<String> broadcast = keyedStream1.broadcast(v);
//        keyedStream.connect(broadcast)
//                        .process(new )

        env.execute();
    }
}
