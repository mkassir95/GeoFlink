package GeoFlink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.63.64.48:9092");
        properties.setProperty("group.id", "flink_consumer");

        // Create Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "r2k_pos2",
                new SimpleStringSchema(),
                properties
        );

        // Add source
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Map function to extract robot ID and speed
        DataStream<Tuple2<String, Double>> idAndSpeedStream = kafkaStream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] parts = value.split("@");
                if (parts.length == 6) {
                    String robotID = parts[0];
                    String speedStr = parts[5];
                    if (speedStr.endsWith("m/s")) {
                        double speed = Double.parseDouble(speedStr.substring(0, speedStr.length() - 4));
                        return new Tuple2<>(robotID, speed);
                    } else {
                        LOG.warn("Speed does not end with 'm/s': " + speedStr);
                        return null;
                    }
                } else {
                    LOG.warn("Unexpected data format: " + value);
                    return null;
                }
            }
        });

        // Reduce to find the maximum speed along with robot ID for each window
        SingleOutputStreamOperator<Tuple2<String, Double>> maxSpeedStream = idAndSpeedStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        // Map to format the output for stdout and Kafka
        SingleOutputStreamOperator<String> resultStreamForStdout = maxSpeedStream.map(new MapFunction<Tuple2<String, Double>, String>() {
            @Override
            public String map(Tuple2<String, Double> value) throws Exception {
                return "Robot ID: " + value.f0 + " - Maximum speed in the last 15 seconds: " + value.f1 + " m/s";
            }
        });

        SingleOutputStreamOperator<String> resultStreamForKafka = maxSpeedStream.map(new MapFunction<Tuple2<String, Double>, String>() {
            @Override
            public String map(Tuple2<String, Double> value) throws Exception {
                return value.f0 + "@" + value.f1 + " m/s"; // Simple format for Kafka
            }
        });

        // Print the detailed results to stdout
        resultStreamForStdout.print();

        // Setup Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "max_speed_avg",          // target topic
                new SimpleStringSchema(), // serialization schema
                properties                // producer config
        );

        // Add sink to send the concise format to Kafka
        resultStreamForKafka.addSink(kafkaProducer);

        // Execute the streaming program
        env.execute("Geo Flink Point Stream");
    }
}
