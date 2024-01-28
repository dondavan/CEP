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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.swisscom.POJOs.Aggregation_Alert_POJO;
import org.swisscom.POJOs.TCI_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.POJOs.nqa_raw_POJO;
import org.swisscom.Processor.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class NQAPipeline {
    private final Properties pros;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    /* List of topic subscribed to  */
    private final String source_topic = "zabbix-nqa-probe";

    public NQAPipeline(Properties pros, String groupID, StreamExecutionEnvironment env){
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
        /* Json Deserializer and Serializer for Data from Kafka Topic */
        JsonDeserializationSchema<nqa_raw_POJO> jsonFormatDe=new JsonDeserializationSchema<>(nqa_raw_POJO.class);
        JsonSerializationSchema<Aggregation_Alert_POJO> Aggregation_Alert_JsonFormatSe=new JsonSerializationSchema<>();


        /*****************************************************************************************

         Sink Configuration

         *****************************************************************************************/
        KafkaSink<Aggregation_Alert_POJO> NoConnectionCPE_Sink = KafkaSink.<Aggregation_Alert_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("aggregation-alerts")
                        .setValueSerializationSchema(Aggregation_Alert_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<Aggregation_Alert_POJO> NoConnectionFOS_Sink = KafkaSink.<Aggregation_Alert_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("aggregation-alerts")
                        .setValueSerializationSchema(Aggregation_Alert_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        /*****************************************************************************************

         Data Stream Configuration

         *****************************************************************************************/
        /* Instantiate a KafkaSource Instance using builder class */
        KafkaSource<nqa_raw_POJO> kafkaSource = KafkaSource.<nqa_raw_POJO>builder()

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics(source_topic)
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(jsonFormatDe)
                .build();

        /* Get a data stream from environment through added Kafka Source*/
        DataStream<nqa_raw_POJO> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), source_topic+"_stream");

        /* Site to Site Failure Aggregation Stream Processing */
        DataStream<Aggregation_Alert_POJO> NoConnectionCPE_Stream = kafkaStream
                .keyBy(value -> value.metrictype)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(300)))
                .trigger(new NoConnectionCPETrigger())
                .process(new NoConnectionCPEProcessor());

        /* Site to Site Failure Aggregation Stream Processing */
        DataStream<Aggregation_Alert_POJO> NoConnectionFOS_Stream = kafkaStream
                .keyBy(value -> value.metrictype)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(300)))
                .trigger(new NoConnectionFOSTrigger())
                .process(new NoConnectionFOSProcessor());

        NoConnectionCPE_Stream.sinkTo(NoConnectionCPE_Sink);
        NoConnectionFOS_Stream.sinkTo(NoConnectionFOS_Sink);

    }

}
