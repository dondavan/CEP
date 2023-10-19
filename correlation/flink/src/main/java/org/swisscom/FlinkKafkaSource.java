package org.swisscom;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaSource {

    private Properties pros = new Properties();

    /* List of topic subscribed to  */
    private List<String> topics;

    /* Consumer group ID */
    String groupID = "Test-group";

    public FlinkKafkaSource(Properties pros,Collection<String> topics, String groupID){
        this.pros = pros;
        this.topics = new ArrayList<String>(topics);
        this.groupID = groupID;
    }

    /* Poll event from kafka topic */
    public void createKafkaSource(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* Instantiate a KafkaSource Instance using builder class */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                /* Security configuration */
                .setProperty("security.protocol", this.pros.getProperty("security.protocol"))
                .setProperty("sasl.mechanism",  this.pros.getProperty("sasl.mechanism"))
                .setProperty("sasl.jaas.config", this.pros.getProperty("sasl.jaas.config"))

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics(this.topics)
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        /* Get a data stream from environment through added Kafka Source*/
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "Test Kafka Flink");

        /* Transformation on data stream*/
        kafkaStream.process(new TestProcessor());

        /* Instantiate a sink for stream processing output*/
        KafkaSink<String> sink = KafkaSink.<String>builder()
                /* Security configuration */
                .setProperty("security.protocol", this.pros.getProperty("security.protocol"))
                .setProperty("sasl.mechanism",  this.pros.getProperty("sasl.mechanism"))
                .setProperty("sasl.jaas.config", this.pros.getProperty("sasl.jaas.config"))

                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))

                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test_sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        kafkaStream.sinkTo(sink);

        /* Execute data stream process*/
        try{
            env.execute("Test Kafka Flink");
        }catch (java.lang.Exception e){
            System.out.println(e.getMessage());
        }
    }
}
