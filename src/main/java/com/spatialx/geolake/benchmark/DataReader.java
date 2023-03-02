package com.spatialx.geolake.benchmark;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Feature;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class DataReader {
    static class DataSet {
        private final String inputFile;
        private final Schema schema;

        private final String smallPredicate;

        private final String largePredicate;

        public String getSmallPredicate() {
            return smallPredicate;
        }

        public String getLargePredicate() {
            return largePredicate;
        }

        public DataSet(String inputFile, Schema schema, String smallPredicate, String largePredicate) {
            this.inputFile = inputFile;
            this.schema = schema;
            this.smallPredicate = smallPredicate;
            this.largePredicate = largePredicate;
        }

        public String getInputFile() {
            return inputFile;
        }
        public Schema getSchema() {
            return schema;
        }

        public String name() {
            return inputFile.split("\\.")[0];
        }
    }

    public final static DataSet BEIJING_SUBWAY_STATION = new DataSet("beijing_subway_station.geojson",
            new Schema(
//                    Types.NestedField.required(0, "stationNameEn", Types.StringType.get()),
//                    Types.NestedField.required(1, "stationNameZH", Types.StringType.get()),
//                    Types.NestedField.required(2, "line", Types.StringType.get()),
//                    Types.NestedField.optional(3, "geometry", Types.GeometryType.get())
                    Types.NestedField.optional(0, "geometry", Types.GeometryType.get())),
            "POLYGON ((-8.608112 41.14896, -8.608112 41.148609, -8.607689 41.148609, -8.607689 41.14896, -8.608112 41.14896))",
            "POLYGON ((-8.6181 41.1525, -8.6181 41.1450, -8.6061 41.1450, -8.6061 41.1525, -8.6181 41.1525))"
    );

    public final static DataSet Portotaxi = new DataSet("portotaxi.geojson",
            new Schema(
//                    Types.NestedField.required(0, "TRIP_ID", Types.LongType.get()),
//                    Types.NestedField.required(1, "CALL_TYPE", Types.StringType.get()),
//                    Types.NestedField.required(2, "ORIGIN_CALL", Types.StringType.get()),
//                    Types.NestedField.required(3, "ORIGIN_STAND", Types.StringType.get()),
//                    Types.NestedField.required(4, "TAXI_ID", Types.LongType.get()),
//                    Types.NestedField.required(5, "TIMESTAMP", Types.StringType.get()),
//                    Types.NestedField.required(6, "DAY_TYPE", Types.StringType.get()),
//                    Types.NestedField.required(7, "MISSING_DATA", Types.BooleanType.get()),
//                    Types.NestedField.optional(8, "geometry", Types.GeometryType.get())
                    Types.NestedField.optional(0, "geometry", Types.GeometryType.get())),
                    // this small predicate matches 1 row group in wkb-bbox(6 row groups) and nested-list(3 row groups)
                    "POLYGON ((-36.91 31.99, -10 40, -10 41, -36.91 41, -36.91 31.99))",
                    // this large predicate matches 3 row groups in wkb-bbox and 2 row group in nested-list
                    "POLYGON ((-36.91 31.99, -15 31.99, -15 42, -36.91 42, -36.91 31.99))"
    );

    public final static DataSet Tiger2018 = new DataSet("tiger_2018_roads.geojson",
            new Schema(
//                    Types.NestedField.optional(0, "LINEARID", Types.StringType.get()),
//                    Types.NestedField.optional(1, "FULLNAME", Types.StringType.get()),
//                    Types.NestedField.optional(2, "RTTYP", Types.StringType.get()),
//                    Types.NestedField.optional(3, "MTFCC", Types.StringType.get()),
//                    Types.NestedField.optional(4, "geometry", Types.GeometryType.get())
                    Types.NestedField.optional(0, "geometry", Types.GeometryType.get())
            ),
            // matches 1 row group in wkb-bbox(29 row groups) and 1 row group nested-list(24 row groups)
            "POLYGON ((-172 20, -172 21, -176 21, -176 20, -172 20))",
            // matches 14 row group in wkb-bbox(29 row groups) and 10 row group nested-list(24 row groups)
            "POLYGON ((-91 20, -91 35, -176 35, -176 20, -91 20))"
    );

    public final static DataSet MsBuildings = new DataSet("ucr_msbuildings.geojson",
            new Schema(Types.NestedField.optional(0, "geometry", Types.GeometryType.get())),
            "POLYGON ((-114.6454 37.6349, -114.6454 36.8375, -113.2969 36.8375, -113.2969 37.6349, -114.6454 37.6349))",
            "POLYGON ((-77.3972 39.0228, -77.3972 38.8146, -77.0240 38.8146, -77.0240 39.0228, -77.3972 39.0228))"
    );

    public FeatureIterator readGeojson(String fileName) throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("source/" + fileName);
        FeatureJSON fjson = new FeatureJSON();
        return fjson.streamFeatureCollection(is);
    }

    public Iterator<GenericRecord> readGeoJson(String fileName, Schema schema) throws IOException {
        List<Types.NestedField> columns = schema.columns();
        FeatureIterator iterator = readGeojson(fileName);
        return new Iterator<GenericRecord> () {
            private long total = 0;
            private long errCount = 0;
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            public void close() {
                iterator.close();
            }
            @Override
            public GenericRecord next() {
                try {
                    Feature feature = iterator.next();
                    GenericRecord record = GenericRecord.create(schema);
                    for (Types.NestedField column : columns) {
                        String name = column.name();
                        if (column.type().typeId() == Type.TypeID.GEOMETRY) {
                            Geometry value = (Geometry) feature.getDefaultGeometryProperty().getValue();
                            record.setField(name, value);
                        } else {
                            record.setField(name, feature.getProperty(name).getValue());
                        }
                    }
                    total += 1;
                    return record;
                } catch (Exception e){
                    errCount += 1;
                    System.out.println("total: " + total + "; error: " + errCount + e.getMessage());
                }
                return null;
            }
        };
    }

    public String output(String inputFile, String encoding) {
        String f = String.format("%s.%s.parquet", inputFile.split("\\.")[0], encoding);
        Path source = Paths.get(Objects.requireNonNull(this.getClass().getResource("/")).getPath());
        Path newFolder = Paths.get(source.toAbsolutePath() + "/output/");
        Path newFile = Paths.get(newFolder.toAbsolutePath() + "/" + f);
        return newFile.toAbsolutePath().toString();
    }

    public static void main(String[] args) throws IOException {
        DataReader dataReader = new DataReader();
        FeatureIterator featureIterator = dataReader.readGeojson(BEIJING_SUBWAY_STATION.inputFile);
        int count = 0;
        while (featureIterator.hasNext() && count < 10) {
            System.out.println(featureIterator.next());
            count++;
        }
        featureIterator.close();
    }
}
