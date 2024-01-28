package org.swisscom.Pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.swisscom.POJOs.TCI_POJO;
import org.swisscom.Processor.StreamFilteringProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ZabbixTCIJointPipeline {
    private final Properties pros;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    /* List of topic subscribed to  */
    private final String source_topic = "tci_zabbix_events";

    public ZabbixTCIJointPipeline(Properties pros, String groupID, StreamExecutionEnvironment env){
        this.pros = pros;
        this.groupID = groupID;
        this.env = env;
        System.out.println(source_topic);
    }

    public void createKafkaSource() {

        /* Setup separate source and sink for different topic, for different deserializer, operator */
        /*****************************************************************************************

         Serialization Configuration

         *****************************************************************************************/
        JsonDeserializationSchema<TCI_POJO> jsonFormatDe=new JsonDeserializationSchema<>(TCI_POJO.class);
        JsonSerializationSchema<TCI_POJO> jsonFormatSe=new JsonSerializationSchema<>();


        /*****************************************************************************************

         Sink Configuration

         *****************************************************************************************/
        /* Instantiate a sink for stream processing output*/
        KafkaSink<TCI_POJO> sink = KafkaSink.<TCI_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))

                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("FILTERED_EVENT_TOPIC")
                        .setValueSerializationSchema(jsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        /*****************************************************************************************

         Data Stream Configuration

         *****************************************************************************************/
        /* Instantiate a KafkaSource Instance using builder class */
        KafkaSource<TCI_POJO> kafkaSource = KafkaSource.<TCI_POJO >builder()

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics(source_topic)
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(jsonFormatDe)
                .build();

        /* Get a data stream from environment through added Kafka Source*/
        DataStream<TCI_POJO> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), source_topic+"_stream");

        /* Transformation on data stream*/
        kafkaStream.process(new StreamFilteringProcessor());
        kafkaStream.sinkTo(sink);

    }

}
