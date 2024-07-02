package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PolygonPointRangeQuery extends RangeQuery<Polygon, Point> {
    public PolygonPointRangeQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Polygon> run(DataStream<Polygon> polygonStream, Set<Point> queryPointSet, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            //Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0]);
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0].gridID);

            HashSet<String> neighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Point point:queryPointSet) {
                neighboringCells.addAll(uGrid.getNeighboringCells(queryRadius, point));
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, point.gridID));
            }

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    //System.out.println(poly);

                    int cellIDCounter = 0;
                    for(String polyGridID: poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if(cellIDCounter == poly.gridIDsSet.size()){
                                collector.collect(poly);
                            }
                        }
                        else { // candidate neighbors

                            double distance;

                            for(Point point:queryPointSet) {

                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, poly);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, poly);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(poly);
                                    break;
                                }
                            }

                            /*
                            if(approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(((Point[])queryPointSet.toArray())[0], poly);
                            }else{
                                distance = DistanceFunctions.getDistance(((Point[])queryPointSet.toArray())[0], poly);
                            }

                            if (distance <= queryRadius){
                                collector.collect(poly);
                            }
                             */
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - POLYGON");
        }
        //--------------- Window-based - POINT - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            //Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0]);
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0].gridID);

            HashSet<String> neighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Point point:queryPointSet) {
                neighboringCells.addAll(uGrid.getNeighboringCells(queryRadius, point));
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, point.gridID));
            }

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            //System.out.println("timeStampMillisec " + p.timeStampMillisec);
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> pointIterator, Collector<Polygon> neighbors) throws Exception {
                            for (Polygon poly : pointIterator) {
                                int cellIDCounter = 0;
                                for(String polyGridID: poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if(cellIDCounter == poly.gridIDsSet.size()){
                                            neighbors.collect(poly);
                                        }
                                    }
                                    else { // candidate neighbors

                                        double distance;
                                        for(Point point:queryPointSet) {

                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, poly);
                                            } else {
                                                distance = DistanceFunctions.getDistance(point, poly);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(poly);
                                                break;
                                            }
                                        }

                            /*
                            if(approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(((Point[])queryPointSet.toArray())[0], poly);
                            }else{
                                distance = DistanceFunctions.getDistance(((Point[])queryPointSet.toArray())[0], poly);
                            }

                            if (distance <= queryRadius){
                                collector.collect(poly);
                            }
                             */
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POLYGON");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
