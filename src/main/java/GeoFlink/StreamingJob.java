package GeoFlink;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
                if (parts.length == 9) {
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

        // Map function to extract robot ID and speed
        DataStream<Tuple5<String, Double,Double,Double,Double>> gpsStream = kafkaStream.map(new MapFunction<String, Tuple5<String, Double,Double,Double,Double>>() {
            @Override
            public Tuple5<String, Double,Double,Double,Double> map(String value) throws Exception {
                String[] parts = value.split("@");
                if (parts.length == 9) {
                    String robotID = parts[0];
                    Double firstLatitude = Double.parseDouble(parts[1]);
                    Double firstLongitude = Double.parseDouble(parts[2]);
                    Double lastLatitude = Double.parseDouble(parts[3]);
                    Double lastLongitude = Double.parseDouble(parts[4]);
                    long firstTimeStamp = (long) Double.parseDouble(parts[6]);
                    long lastTimeStamp = (long) Double.parseDouble(parts[7]);
                    long esperProcessingTime=(long) Double.parseDouble(parts[8]);

                    // Define the bounding box for your grid based on the provided coordinates
                    double gridMinX = 3.433594;
                    double gridMinY = 46.339055;
                    double gridMaxX = 3.433618;
                    double gridMaxY = 46.339102;

                    // Define the cell length in meters
                    double cellLengthMeters = 0.1; // Adjust this value based on the desired resolution

                    // Create the UniformGrid
                    UniformGrid uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);



                    Point firstPoint = new Point(robotID, firstLatitude, firstLongitude, firstTimeStamp, uGrid,esperProcessingTime);
                    Point lastPoint = new Point(robotID, lastLatitude, lastLongitude, lastTimeStamp, uGrid,esperProcessingTime);


                    //System.out.println("Point 1: " + firstPoint.toString());
                    //System.out.println("Point 2: " + lastPoint.toString());



                    return new Tuple5<>(robotID, firstLatitude,firstLongitude,lastLatitude,lastLongitude);

                }
                return null;
            }
        });





        // Print the GPS stream to stdout
        //gpsStream.print();


        // Map function to extract robot ID and GPS data and create Point objects
        DataStream<Point> pointStream = kafkaStream.flatMap(new FlatMapFunction<String, Point>() {
            @Override
            public void flatMap(String value, Collector<Point> out) throws Exception {
                try {
                    String[] parts = value.split("@");
                    if (parts.length == 9) {
                        String robotID = parts[0];
                        Double firstLatitude = Double.parseDouble(parts[1]);
                        Double firstLongitude = Double.parseDouble(parts[2]);
                        Double lastLatitude = Double.parseDouble(parts[3]);
                        Double lastLongitude = Double.parseDouble(parts[4]);
                        long firstTimeStamp = (long) Double.parseDouble(parts[6]);
                        long lastTimeStamp = (long) Double.parseDouble(parts[7]);
                        long esperProcessingTime=(long) Double.parseDouble(parts[8]);

                        // Define the bounding box for your grid based on the provided coordinates
                        double gridMinX = 3.433594;
                        double gridMinY = 46.339055;
                        double gridMaxX = 3.433618;
                        double gridMaxY = 46.339102;

                        // Define the cell length in meters
                        double cellLengthMeters = 0.1; // Adjust this value based on the desired resolution

                        // Create the UniformGrid
                        UniformGrid uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);

                        // Create the Point objects
                        Point firstPoint = new Point(robotID, firstLatitude, firstLongitude, firstTimeStamp, uGrid,esperProcessingTime);
                        Point lastPoint = new Point(robotID, lastLatitude, lastLongitude, lastTimeStamp, uGrid,esperProcessingTime);

                        // Collect both points
                        out.collect(firstPoint);
                        out.collect(lastPoint);

                    }
                } catch (Exception e) {
                    LOG.error("Error parsing GPS data: " + value, e);
                }
            }
        });

        // Print the Point stream to stdout
        //pointStream.print();

        // Kafka producer properties for alerts
        Properties alertProperties = new Properties();
        alertProperties.setProperty("bootstrap.servers", "10.63.64.48:9092");
        alertProperties.setProperty("group.id", "flink_producer");
        alertProperties.setProperty("acks", "all");

        // Setup Kafka producer for alerts
        FlinkKafkaProducer<String> alertProducer = new FlinkKafkaProducer<>(
                "alert_geoflink",
                new SimpleStringSchema(),
                alertProperties
        );

        DataStream<String> windowedStream = pointStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new ProcessAllWindowFunction<Point, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Point> elements, Collector<String> out) throws Exception {
                        long startTime = System.currentTimeMillis(); // Start timing
                        List<Point> points = new ArrayList<>();
                        for (Point point : elements) {
                            points.add(point);
                        }

                        // Define a map to store the minimum distance between each pair of trajectories
                        Map<String, Double> minDistances = new HashMap<>();
                        Map<String, Tuple2<Point, Point>> closestPoints = new HashMap<>();

                        // Iterate over all points and find the minimum distance for each pair of trajectories
                        for (Point point1 : points) {
                            for (Point point2 : points) {
                                if (!point1.objID.equals(point2.objID)) {
                                    String trajectoryPairKey = point1.objID + "-" + point2.objID;

                                    // Calculate the distance between point1 and point2
                                    double distance = DistanceFunctions.getDistance(point1, point2);

                                    // Update the minimum distance for this trajectory pair
                                    if (!minDistances.containsKey(trajectoryPairKey) || distance < minDistances.get(trajectoryPairKey)) {
                                        minDistances.put(trajectoryPairKey, distance);
                                        closestPoints.put(trajectoryPairKey, new Tuple2<>(point1, point2));
                                    }
                                }
                            }
                        }

                        // Output the result with the minimum distance for each trajectory pair
                        for (Map.Entry<String, Double> entry : minDistances.entrySet()) {
                            String trajectoryPairKey = entry.getKey();
                            double minDistance = entry.getValue();
                            Point point1 = closestPoints.get(trajectoryPairKey).f0;
                            Point point2 = closestPoints.get(trajectoryPairKey).f1;
                            System.out.println("Esper+GeoFlink Minimum distance between " + point1.objID + " and " + point2.objID + " in the last 15 seconds is: " + minDistance + " meters");
                            long endTime = System.currentTimeMillis(); // End timing
                            long executionTime = endTime - startTime+points.get(0).processingTime; // Calculate execution time
                            System.out.println("Esper+GeoFlink Window Execution Time: " + executionTime + " ms"); // Output execution
                            String output = point1.objID + "," + point2.objID + "," + minDistance+","+executionTime;
                            out.collect(output);
                        }
                    }
                });

        //windowedStream.print();
        windowedStream.addSink(alertProducer);
        // Define the output path for the CSV file
        String outputPath1 = "/home/mkassir/esper+geoflink_trajectory_distance.csv";

// Write the formatted stream to a CSV file
        windowedStream.writeAsText(outputPath1, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Create Kafka consumer for "r2k_pos"
        FlinkKafkaConsumer<String> kafkaConsumerR2KPos = new FlinkKafkaConsumer<>(
                "r2k_pos",
                new SimpleStringSchema(),
                properties
        );

        // Add source for "r2k_pos"
        DataStream<String> streamFromR2KPos = env.addSource(kafkaConsumerR2KPos);

        DataStream<Point> parsedStreamR2KPos = streamFromR2KPos.flatMap(new FlatMapFunction<String, Point>() {
            @Override
            public void flatMap(String value, Collector<Point> out) throws Exception {
                try {
                    String[] parts = value.split("\\s+");
                    if (parts.length >=8) {
                        Long timestamp = Long.parseLong(parts[0]);
                        String role = parts[1];
                        Double latitude = Double.parseDouble(parts[3]);
                        Double longitude = Double.parseDouble(parts[4]);

                        // Define the bounding box for your grid based on the provided coordinates
                        double gridMinX = 3.433594;
                        double gridMinY = 46.339055;
                        double gridMaxX = 3.433618;
                        double gridMaxY = 46.339102;

                        // Define the cell length in meters
                        double cellLengthMeters = 0.1; // Adjust this value based on the desired resolution

                        // Create the UniformGrid
                        UniformGrid uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);

                        // Create the Point objects
                        Point point = new Point(role, latitude, longitude, timestamp, uGrid);

                        // Collect both points
                        out.collect(point);

                    }
                } catch (Exception e) {
                    LOG.error("Error parsing GPS data: " + value, e);
                }
            }
        });



// Print all parsed data
       // parsedStreamR2KPos
              //  .filter(value -> value != null) // Optional: Ensure no null values are printed
              //  .print();
        DataStream<String> geoFlinkWindowedStream = parsedStreamR2KPos
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new ProcessAllWindowFunction<Point, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Point> elements, Collector<String> out) throws Exception {
                        long startTime = System.currentTimeMillis(); // Start timing
                        List<Point> points = new ArrayList<>();
                        for (Point point : elements) {
                            points.add(point);
                        }

                        // Define a map to store the minimum distance between each pair of trajectories
                        Map<String, Double> minDistances = new HashMap<>();
                        Map<String, Tuple2<Point, Point>> closestPoints = new HashMap<>();

                        // Iterate over all points and find the minimum distance for each pair of trajectories
                        for (Point point1 : points) {
                            for (Point point2 : points) {
                                if (!point1.objID.equals(point2.objID)) {
                                    String trajectoryPairKey = point1.objID + "-" + point2.objID;

                                    // Calculate the distance between point1 and point2
                                    double distance = DistanceFunctions.getDistance(point1, point2);

                                    // Update the minimum distance for this trajectory pair
                                    if (!minDistances.containsKey(trajectoryPairKey) || distance < minDistances.get(trajectoryPairKey)) {
                                        minDistances.put(trajectoryPairKey, distance);
                                        closestPoints.put(trajectoryPairKey, new Tuple2<>(point1, point2));
                                    }
                                }
                            }
                        }

                        // Output the result with the minimum distance for each trajectory pair
                        for (Map.Entry<String, Double> entry : minDistances.entrySet()) {
                            String trajectoryPairKey = entry.getKey();
                            double minDistance = entry.getValue();
                            Point point1 = closestPoints.get(trajectoryPairKey).f0;
                            Point point2 = closestPoints.get(trajectoryPairKey).f1;
                            System.out.println("GeoFlink Minimum distance between " + point1.objID + " and " + point2.objID + " in the last 15 seconds is: " + minDistance + " meters");
                            long endTime = System.currentTimeMillis(); // End timing
                            long executionTime = endTime - startTime; // Calculate execution time
                            System.out.println("GeoFlink Window Execution Time: " + executionTime + " ms"); // Output execution time
                            String output = point1.objID + "," + point2.objID + "," + minDistance+","+executionTime;
                            out.collect(output);
                        }

                    }
                });



        // Kafka producer properties for alerts


        // Setup Kafka producer for alerts
        FlinkKafkaProducer<String> geoFlinkProducer = new FlinkKafkaProducer<>(
                "distances_2geoflink",
                new SimpleStringSchema(),
                alertProperties
        );
        geoFlinkWindowedStream.addSink(geoFlinkProducer);

        // Define the output path for the CSV file
        String outputPath = "/home/mkassir/geoflink_trajectory_distance.csv";

// Write the formatted stream to a CSV file
        geoFlinkWindowedStream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);


// Assuming Point has a getRobotID method or similar identifier.
        KeyedStream<String, String> keyedWindowedStream = windowedStream
                .keyBy(record -> record.split(",")[0]);  // Assuming the robotID is the first element in the comma-separated string.

        KeyedStream<String, String> keyedGeoFlinkWindowedStream = geoFlinkWindowedStream
                .keyBy(record -> record.split(",")[0]);  // Assuming the structure.
        DataStream<String> joinedStream = keyedWindowedStream
                .connect(keyedGeoFlinkWindowedStream)
                .process(new CoProcessFunction<String, String, String>() {
                    private ValueState<String> stream1State;

                    @Override
                    public void open(Configuration config) {
                        stream1State = getRuntimeContext().getState(new ValueStateDescriptor<>("stream1", String.class));
                    }

                    @Override
                    public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                        // Store the data from the first stream (stream1)
                        stream1State.update(value);
                        // Set a timer to clear state later if needed
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000);  // Adjust as necessary
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        // When the second stream data arrives, check for the first stream's state
                        String stream1Value = stream1State.value();
                        if (stream1Value != null) {
                            // Collect output only when both streams have data
                            out.collect("Joined: " + stream1Value + " & " + value);
                            // Optionally clear state after the join
                            stream1State.clear();
                        }
                        // You can decide whether to store the second stream state if needed.
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        // Clear state after the timer fires
                        stream1State.clear();
                    }
                });


        joinedStream.print();
// Define the output path for the CSV file
        String outputPath2 = "/home/mkassir/new_result.csv";

// Write the formatted stream to a CSV file
        joinedStream.writeAsText(outputPath2, FileSystem.WriteMode.OVERWRITE).setParallelism(1);




        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);






        // Execute the streaming program
        env.execute("Geo Flink Point Stream");
    }
}
