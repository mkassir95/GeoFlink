package GeoFlink.spatialOperators.knn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class PointPolygonKNNQuery extends KNNQuery<Point, Polygon> {
    public PointPolygonKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeKNNQuery(conf, index);
    }

    public DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> run(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k) throws IOException {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            //return realTime(pointStream, queryPolygon, queryRadius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
            return windowBased(pointStream, queryPolygon, queryRadius, k, omegaJoinDurationSeconds, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
        }

        //--------------- Real-time Naive - POINT - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.RealTimeNaive) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTimeNaive(pointStream, queryPolygon, queryRadius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, queryPolygon, queryRadius, k, windowSize, windowSlideStep, uGrid, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    public DataStream<Long> runLatency(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k) throws IOException {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTimeLatency(pointStream, queryPolygon, queryRadius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBasedLatency(pointStream, queryPolygon, queryRadius, k, windowSize, windowSlideStep, uGrid, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    /*
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> realTime(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery) throws IOException {

        HashSet<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        HashSet<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
        candidateNeighboringCells.addAll(guaranteedNeighboringCells);

        DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                //return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                return (candidateNeighboringCells.contains(point.gridID));
            }
        }).startNewChain();

        DataStream<Point> filteredPointStreamWithTsAndWm =
                filteredPoints.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = filteredPointStreamWithTsAndWm.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.gridID;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point point : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                }
                            } else {
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                    }
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        //Output kNN Stream
        return windowedKNN
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new kNNWinAllEvaluationPointStream(k));
    }*/

    // REAL-TIME Naive
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> realTimeNaive(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery) throws IOException {

        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = pointStreamWithTsAndWm.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.gridID;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point point : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                }
                            } else {
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                    }
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        //Output kNN Stream
        return windowedKNN
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new kNNWinAllEvaluationPointStream(k));
    }

    // WINDOW BASED
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> windowBased(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);


        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point point : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                }
                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }
                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                    }
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -

        //Output kNN Stream
        return windowedKNN
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new kNNWinAllEvaluationPointStream(k));
    }

    // REAL-TIME Latency
    private DataStream<Long> realTimeLatency(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k, int omegaJoinDurationSeconds, UniformGrid uGrid, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<Long> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.gridID;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<Point, Long, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Long> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point point : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                }

                                Date date = new Date();
                                Long latency = date.getTime() -  point.timeStampMillisec;
                                outputStream.collect(latency);

                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {

                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Point, Double>(point, distance));

                                        Date date = new Date();
                                        Long latency = date.getTime() - point.timeStampMillisec;
                                        outputStream.collect(latency);
                                    }
                                }
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");


        //Output kNN Stream
        return windowedKNN;
    }

    // WINDOW BASED Latency
    private DataStream<Long> windowBasedLatency(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);


        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        //Output kNN Stream
        return filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, Long, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Long> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point point : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Point, Double>(point, distance));
                                }

                                Date date = new Date();
                                Long latency = date.getTime() -  point.timeStampMillisec;
                                outputStream.collect(latency);

                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                }else{
                                    distance = DistanceFunctions.getDistance(point, queryPolygon);
                                }
                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Point, Double>(point, distance));

                                        Date date = new Date();
                                        Long latency = date.getTime() - point.timeStampMillisec;
                                        outputStream.collect(latency);
                                    }
                                }
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");
    }
}

