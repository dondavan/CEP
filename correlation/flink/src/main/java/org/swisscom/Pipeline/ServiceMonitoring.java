package org.swisscom.Pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.swisscom.POJOs.TCI_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.Processor.ServiceMonitoringProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ServiceMonitoring {
    private final Properties pros;

    /* List of topic subscribed to  */
    private final List<String> topics;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    public ServiceMonitoring(Properties pros, Collection<String> topics, String groupID, StreamExecutionEnvironment env){
        this.pros = pros;
        this.topics = new ArrayList<String>(topics);
        this.groupID = groupID;
        this.env = env;
        System.out.println(topics);
    }

    public void createKafkaSource() {

        /* Setup separate source and sink for different topic, for different deserializer, operator */
        for (String topic:topics) {
            JsonDeserializationSchema<Zabbix_events_POJO> jsonFormatDe=new JsonDeserializationSchema<>(Zabbix_events_POJO.class);
            JsonSerializationSchema<Zabbix_events_POJO> jsonFormatSe=new JsonSerializationSchema<>();

            /* Instantiate a KafkaSource Instance using builder class */
            KafkaSource<Zabbix_events_POJO> kafkaSource = KafkaSource.<Zabbix_events_POJO>builder()

                    /* Server Topic GroupID */
                    .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                    .setTopics(topic)
                    .setGroupId(this.groupID)

                    /* Consumer behavior */
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(jsonFormatDe)
                    .build();

            /* Get a data stream from environment through added Kafka Source*/
            DataStream<Zabbix_events_POJO> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), topic+"_stream");

            /* Transformation on data stream*/
            kafkaStream.process(new ServiceMonitoringProcessor());

            /* Instantiate a sink for stream processing output*/
            KafkaSink<Zabbix_events_POJO> sink = KafkaSink.<Zabbix_events_POJO>builder()
                    .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))

                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic+"_stream")
                            .setValueSerializationSchema(jsonFormatSe)
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            kafkaStream.sinkTo(sink);

        }
    }
}
