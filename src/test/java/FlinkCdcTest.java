import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by 铁盾 on 2022/10/24
 */
public class FlinkCdcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("tms")
                .tableList("tms_config.tms_config_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> configSource =
                env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        configSource
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
