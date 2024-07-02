package GeoFlink.spatialOperators.knn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PolygonLineStringKNNQuery extends KNNQuery<Polygon, LineString> {
    public PolygonLineStringKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeKNNQuery(conf, index);
    }

    public DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> run(DataStream<Polygon> polygonStream, LineString queryLineString, double queryRadius, Integer k) throws IOException {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POLYGON - LINESTRING -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            //return realTime(polygonStream, queryLineString, queryRadius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
            return windowBased(polygonStream, queryLineString, queryRadius, k, uGrid, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POLYGON - LINESTRING -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(polygonStream, queryLineString, queryRadius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    /*
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> realTime(DataStream<Polygon> polygonStream, LineString queryLineString, double queryRadius, Integer k, UniformGrid uGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<Polygon> streamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();


        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

        DataStream<PriorityQueue<Tuple2<Polygon, Double>>> windowedKNN = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new WindowFunction<Polygon, PriorityQueue<Tuple2<Polygon, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQ = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> inputTuples, Collector<PriorityQueue<Tuple2<Polygon, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Polygon poly : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, poly.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(poly, queryLineString);
                                }
                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                                }
                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, poly.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(poly, queryLineString);
                                }
                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
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
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new kNNWinAllEvaluationPolygonStream(k));
    }*/

    // WINDOW BASED
    private DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> windowBased(DataStream<Polygon> polygonStream, LineString queryLineString, double queryRadius, Integer k, UniformGrid uGrid, int windowSize, int windowSlideStep, int allowedLateness, boolean approximateQuery) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<Polygon> streamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();


        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

        DataStream<PriorityQueue<Tuple2<Polygon, Double>>> windowedKNN = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Polygon, PriorityQueue<Tuple2<Polygon, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQ = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> inputTuples, Collector<PriorityQueue<Tuple2<Polygon, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Polygon poly : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, poly.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(poly, queryLineString);
                                }

                                if(distance <= queryRadius) {
                                    kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                                }
                            } else {
                                if(approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, poly.boundingBox);
                                }else{
                                    distance = DistanceFunctions.getDistance(poly, queryLineString);
                                }

                                if(distance <= queryRadius) {
                                    // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                    assert kNNPQ.peek() != null;
                                    double largestDistInPQ = kNNPQ.peek().f1;

                                    if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                        kNNPQ.poll();
                                        kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
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
                .apply(new kNNWinAllEvaluationPolygonStream(k));
    }
}
