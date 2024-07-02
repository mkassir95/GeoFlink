package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LineStringPolygonRangeQuery extends RangeQuery<LineString, Polygon> {
    public LineStringPolygonRangeQuery(QueryConfiguration conf, SpatialIndex index){
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<LineString> run(DataStream<LineString> lineStringStream, Set<Polygon> queryPolygonSet, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[])queryPolygonSet.toArray())[0]);
            //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[])queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
            //Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Polygon polygon:queryPolygonSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, polygon));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, polygon, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new CellBasedLineStringFlatMap(neighboringCells)).startNewChain();

            return filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).flatMap(new FlatMapFunction<LineString, LineString>() {
                @Override
                public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String lineStringGridID : lineString.gridIDsSet) {
                        if (guaranteedNeighboringCells.contains(lineStringGridID)){
                            cellIDCounter++;
                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                collector.collect(lineString);
                            }
                        }
                        else { // candidate neighbors
                            double distance;

                            for(Polygon polygon:queryPolygonSet) {

                                if (approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(polygon.boundingBox, lineString.boundingBox);
                                } else {
                                    distance = DistanceFunctions.getDistance(polygon, lineString);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(lineString);
                                    break;
                                }

                                /*
                                if(approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(((Polygon[])queryPolygonSet.toArray())[0].boundingBox, lineString.boundingBox);
                            }else{
                                distance = DistanceFunctions.getDistance(((Polygon[])queryPolygonSet.toArray())[0], lineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(lineString);
                            }*/

                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - LINESTRING - POLYGON");
        }
        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[])queryPolygonSet.toArray())[0]);
            //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[])queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
            //Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Polygon polygon:queryPolygonSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, polygon));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, polygon, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new CellBasedLineStringFlatMap(neighboringCells));

            return filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> lineStringIterator, Collector<LineString> neighbors) throws Exception {

                            for (LineString lineString : lineStringIterator) {
                                int cellIDCounter = 0;
                                for (String lineStringGridID : lineString.gridIDsSet) {
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)){
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                        }
                                    }
                                    else { // candidate neighbors
                                        double distance;
                                        for(Polygon polygon:queryPolygonSet) {

                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(polygon.boundingBox, lineString.boundingBox);
                                            } else {
                                                distance = DistanceFunctions.getDistance(polygon, lineString);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(lineString);
                                                break;
                                            }

                                /*
                                if(approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(((Polygon[])queryPolygonSet.toArray())[0].boundingBox, lineString.boundingBox);
                            }else{
                                distance = DistanceFunctions.getDistance(((Polygon[])queryPolygonSet.toArray())[0], lineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(lineString);
                            }*/

                                        }
                                        break;
                                    }
                                }
                            }

                        }
                    }).name("Window-based - LINESTRING - POLYGON");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
