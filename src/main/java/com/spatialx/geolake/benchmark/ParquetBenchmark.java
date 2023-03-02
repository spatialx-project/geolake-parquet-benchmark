package com.spatialx.geolake.benchmark;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.arrow.vectorized.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import static com.spatialx.geolake.benchmark.DataReader.*;
import static org.apache.iceberg.TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING;

public class ParquetBenchmark {
    static File warehouse;

    private final DataReader.DataSet dataSet;
    private final String inputFile;
    private final Schema schema;
    public static final Configuration conf = new Configuration();

    public ParquetBenchmark(DataSet dataSet) {
        this.dataSet = dataSet;
        this.inputFile = dataSet.getInputFile();
        this.schema = dataSet.getSchema();
    }
    static String warehousePath;
    static HadoopCatalog catalog = null;
    public String encoding;

    private TableIdentifier tableName;
    private static Table table = null;

    public int batchSize = 5000;

    private long writeTime = 0;

    private String dataFilePath;

    public void setUp() throws IOException {
        createWarehouse();
        createTable();
    }

    private void createWarehouse() throws IOException {
        warehouse = File.createTempFile("warehouse", null);
        warehouse.delete();
        warehousePath = (new Path(warehouse.getAbsolutePath())).toString();
        System.out.println("warehousePath: " + warehousePath);
        System.out.println("encoding: " + encoding);
        catalog = new HadoopCatalog(conf, warehousePath);
    }

    private void createTable() throws IOException {
        tableName = TableIdentifier.of("test", dataSet.name() + "_" + encoding);
        if (!catalog.tableExists(tableName)) {
            System.out.println("create table with encoding: " + encoding);
            Map<String, String> properties = new HashMap<>();
            properties.put(PARQUET_GEOMETRY_WRITE_ENCODING, encoding);
            PartitionSpec spec = PartitionSpec.builderFor(schema).build();
            table = catalog.createTable(tableName, schema, spec, properties);
            writeTime = prepareTable();
        } else {
            table = catalog.loadTable(tableName);
        }
    }

    private long prepareTable() throws IOException {
        dataFilePath = String.format(warehousePath +  "/" + dataSet.name() + "-" + encoding + ".parquet");
        OutputFile outputFile = table.io().newOutputFile(dataFilePath);
        System.out.println("outputFile: " + outputFile.location());
        DataWriter<Object> writer =
            Parquet.writeData(outputFile)
                .forTable(table)
                .createWriterFunc(
                    type -> GenericParquetWriter.buildWriter(table.schema(), type, table.properties()))
                .build();
        DataReader dataReader = new DataReader();
        long start = System.currentTimeMillis();
        Iterator<GenericRecord> records = dataReader.readGeoJson(inputFile, schema);
        while (records.hasNext()) {
            Record record = records.next();
            if (record != null) {
                writer.write(record);
            }
        }
        writer.close();
        long end = System.currentTimeMillis();
        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
        System.out.println("write parquet file cost: " + (end - start) + " ms");
        return end - start;
    }

    public void dropWarehouse() throws IOException {
        if (warehouse != null && warehouse.exists()) {
            Path path = new Path(warehouse.getAbsolutePath());
            FileSystem fs = path.getFileSystem(conf);
            fs.delete(path, true);
        }
    }

    public void readAll() throws IOException {
        readTable(Expressions.alwaysTrue());
    }

    public void vectorizedReadAll() throws IOException {
        vectorizedReadTable(Expressions.alwaysTrue());
    }

    public List<Record> readWithPredicate(String predicate) throws IOException, ParseException {
        Geometry bbox = new WKTReader().read(predicate);
        readTable(Expressions.stCoveredBy("geometry", bbox));
        return null;
    }

    public List<Record> vectorizedReadWithPredicate(String predicate) throws IOException, ParseException {
        Geometry bbox = new WKTReader().read(predicate);
        vectorizedReadTable(Expressions.stCoveredBy("geometry", bbox));
        return null;
    }

    private void readTable(Expression expression) throws IOException {
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table).where(expression);
        long count = 0;
        try (CloseableIterable<Record> result = scanBuilder.build()) {
            for (Record r: result) {
                count++;
            }
        }
        System.out.println("readTable count: " + count);
    }

    private void vectorizedReadTable(Expression expression)
        throws IOException {
        long count = 0;
        List<Types.NestedField> columns = schema.columns();
        try (VectorizedTableScanIterable itr =
                 new VectorizedTableScanIterable(table.newScan().filter(expression), batchSize, true)) {
            for (ColumnarBatch batch : itr) {
                int rows = batch.numRows();
                List<Record> batchRecords = new ArrayList<>(rows);
                for (int i = 0; i < rows; i++) {
                    batchRecords.add(GenericRecord.create(schema));
                }
                IntStream.range(0, columns.size()).forEach(i -> {
                    String name = columns.get(i).name();
                    switch (columns.get(i).type().typeId()) {
                        case STRING:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getString(r)));
                            break;
                        case INTEGER:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getInt(r)));
                            break;
                        case LONG:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getLong(r)));
                            break;
                        case DOUBLE:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getDouble(r)));
                            break;
                        case FLOAT:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getFloat(r)));
                            break;
                        case BOOLEAN:
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, batch.column(i).getBoolean(r)));
                            break;
                        case GEOMETRY:
                            List<Geometry> geometries = columnVectorToGeometryList(batch.column(i));
                            IntStream.range(0, rows).forEach(r -> batchRecords.get(r).setField(name, geometries.get(r)));
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported type: " + columns.get(i).type());
                    }
                });
                count += batchRecords.size();
            }
        }
        System.out.println("vectorizedReadTable count: " + count);
    }

    private List<Geometry> columnVectorToGeometryList(ColumnVector geomVector) {
        Assert.assertTrue(
            "Column vector should be a geometry vector",
            geomVector.getVectorHolder() instanceof VectorHolder.GeometryVectorHolder);
        VectorHolder.GeometryVectorHolder geomVectorHolder = (VectorHolder.GeometryVectorHolder) geomVector.getVectorHolder();
        List<Geometry> geometries = null;
        switch (geomVectorHolder.getGeometryVectorEncoding()) {
            case "wkb":
                VarBinaryVector wkbVector = (VarBinaryVector) geomVector.getFieldVector();
                geometries = wkbVectorToGeometryList(wkbVector);
                break;
            case "wkb-bbox":
                StructVector wkbBBOXVector = (StructVector) geomVector.getFieldVector();
                geometries = wkbVectorToGeometryList((VarBinaryVector) wkbBBOXVector.getChild("wkb"));
                break;
            case "nested-list":
                geometries = nestedListVectorToGeometryList((StructVector) geomVector.getFieldVector());
                break;
            default:
                throw new IllegalArgumentException("Unknown geometry encoding" + geomVectorHolder.getGeometryVectorEncoding());
        }
        return geometries;
    }

    private List<Geometry> wkbVectorToGeometryList(VarBinaryVector wkbVector) {
        int valueCount = wkbVector.getValueCount();
        List<Geometry> geometries = new ArrayList<>(valueCount);
        for (int k = 0; k < valueCount; k++) {
            if (wkbVector.isNull(k)) {
                geometries.add(null);
            } else {
                byte[] wkb = wkbVector.get(k);
                try {
                    Geometry geometry = new WKBReader().read(wkb);
                    geometries.add(geometry);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return geometries;
    }

    private List<Geometry> nestedListVectorToGeometryList(StructVector vec) {
        int valueCount = vec.getValueCount();
        List<Geometry> geometries = new ArrayList<>(valueCount);
        ArrowGeometryVectorAccessor.NestedListAccessor accessor =
            new ArrowGeometryVectorAccessor.NestedListAccessor(vec);
        for (int k = 0; k < valueCount; k++) {
            if (vec.isNull(k)) {
                geometries.add(null);
            } else {
                geometries.add(accessor.getGeometry(k));
            }
        }
        return geometries;
    }

    public void run() throws IOException, ParseException {
        List<HashMap<String, Object>> results = new ArrayList<>();
        for (String encoding : Arrays.asList("wkb", "wkb-bbox", "nested-list")) {
            HashMap<String, Object> metrics = new HashMap<>();
            metrics.put("encoding", encoding);
            ParquetBenchmark benchmark = new ParquetBenchmark(dataSet);
            benchmark.encoding = encoding;
            benchmark.setUp();
            metrics.put("write_time_in_ms", benchmark.writeTime);
            if (benchmark.dataFilePath != null) {
                metrics.put("filesize_in_bytes", new File(benchmark.dataFilePath).length());
            }
            List<Long> readAllTimes = new ArrayList<>();
            List<Long> vectorizedReadAllTimes = new ArrayList<>();
            List<Long> readSmallPredicateTimes = new ArrayList<>();
            List<Long> vectorizedReadSmallPredicateTimes = new ArrayList<>();
            List<Long> readLargePredicateTimes = new ArrayList<>();
            List<Long> vectorizedReadLargePredicateTimes = new ArrayList<>();
            int readTimes = 5;
            for (int i = 0; i < readTimes; i++) {
                long start = System.currentTimeMillis();
                benchmark.readAll();
                long readAllTime = System.currentTimeMillis();
                benchmark.vectorizedReadAll();
                long vectorizedReadAllTime = System.currentTimeMillis();
                readAllTimes.add(readAllTime - start);
                vectorizedReadAllTimes.add(vectorizedReadAllTime - readAllTime);
//                // for small predicate
                benchmark.readWithPredicate(dataSet.getSmallPredicate());
                long readSmallPredicateTime = System.currentTimeMillis();
                benchmark.vectorizedReadWithPredicate(dataSet.getSmallPredicate());
                long vectorizedReadSmallPredicateTime = System.currentTimeMillis();
                readSmallPredicateTimes.add(readSmallPredicateTime - vectorizedReadAllTime);
                vectorizedReadSmallPredicateTimes.add(vectorizedReadSmallPredicateTime - readSmallPredicateTime);

                // for large predicate
                benchmark.readWithPredicate(dataSet.getLargePredicate());
                long readLargePredicateTime = System.currentTimeMillis();
                benchmark.vectorizedReadWithPredicate(dataSet.getLargePredicate());
                long vectorizedReadLargePredicateTime = System.currentTimeMillis();
                readLargePredicateTimes.add(readLargePredicateTime - vectorizedReadSmallPredicateTime);
                vectorizedReadLargePredicateTimes.add(vectorizedReadLargePredicateTime - readLargePredicateTime);

                System.out.println("encoding: " + encoding + "; readAllTime: " + (readAllTime - start));
                System.out.println("encoding: " + encoding + "; vectorizedReadAllTime: " + (vectorizedReadAllTime - readAllTime));
            }
            metrics.put("read_all_time_in_ms", readAllTimes);
            metrics.put("vectorized_read_all_time_in_ms", vectorizedReadAllTimes);
            metrics.put("read_small_predicate_time_in_ms", readSmallPredicateTimes);
            metrics.put("vectorized_read_small_predicate_time_in_ms", vectorizedReadSmallPredicateTimes);
            metrics.put("read_large_predicate_time_in_ms", readLargePredicateTimes);
            metrics.put("vectorized_read_large_predicate_time_in_ms", vectorizedReadLargePredicateTimes);
            benchmark.dropWarehouse();
            results.add(metrics);
        }
        for (HashMap<String, Object> result : results) {
            System.out.println("------------------------------");
            result.forEach((k, v) -> System.out.println(k + ": " + v));
        }
    }
    public static void main(String[] args) throws IOException, ParseException {
        DataSet ds = BEIJING_SUBWAY_STATION;
        System.out.println(Arrays.toString(args));
        if (args.length > 0) {
            String dsname = args[0];
            switch (dsname) {
                case "portotaix": ds = Portotaxi; break;
                case "tiger": ds = Tiger2018; break;
                case "msbuildings": ds = MsBuildings; break;
                default: throw new IllegalArgumentException("dataset could only be one of [`portotaxi`, `tiger`, `msbuildings`]");
            }
        }
        new ParquetBenchmark(ds).run();
    }
}
