import java.util.*;
import org.voltdb.*;
import org.voltdb.client.*;

public class VoltDBMemoryReport {


    // constants for sizing formulas
    public static final int INLINE_POINTER = 8;
    public static final int STRING_REF_OBJ = 8+8;
    public static final int ON_POOL_OVERHEAD = 8+4; // reverse pointer + length

    public static final int SAMPLE_SIZE = 500000;

    public static final String LINE = new String(new char[100]).replace("\0","-");
    public static final String LINE_THICK = new String(new char[100]).replace("\0","=");
    public static final boolean includeIndex = true;

    Client client;
    StatisticsTABLE statsTable;
    StatisticsINDEX statsIndex;
    String table;

    public static float asMB(long bytes) {
        return (float)bytes/1048576;
    }
    public static float asGB(long bytes) {
        return (float)bytes/1073741824;
    }
    public static long roundToNearestBlockSize(long input) {
        long actualSize = input+12;
        long[] sizes = { 1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192,
                         256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144,
                         8192, 12288, 16384, 24576, 32768, 49152, 65536, 98304,
                         131072, 196608, 262144, 393216, 524288, 786432, 1048576};
        long output = 0;
        for (int i=0; i<sizes.length; i++) {
            if(actualSize <= sizes[i]) {
                output = sizes[i];
                break;
            }
        }
        return output;
    }

    public static long calcVarSize(Long length) {
        if (length == null)
            return INLINE_POINTER;

        return roundToNearestBlockSize(length + ON_POOL_OVERHEAD) + STRING_REF_OBJ + INLINE_POINTER;
    }

    // Calls @Statistics TABLE and stores aggregates maps for each table by name
    // Call print(String tablename) to print the report for each table
    public class StatisticsTABLE {

        public Map<String,Long> tupleCount = new HashMap<String,Long>();
        public Map<String,Long> tupleAllocatedKB = new HashMap<String,Long>();
        public Map<String,Long> tupleKB = new HashMap<String,Long>();
        public Map<String,Long> stringKB = new HashMap<String,Long>();

        public StatisticsTABLE(Client client) throws Exception {

            VoltTable t = client.callProcedure("@Statistics","TABLE",0).getResults()[0];
            while (t.advanceRow()) {
                String name = t.getString(5);

                long tuples = t.getLong(7);
                if (tupleCount.containsKey(name))
                    tuples += tupleCount.get(name);
                tupleCount.put(name,tuples);

                long tupleAllocated = t.getLong(8);
                if (tupleAllocatedKB.containsKey(name))
                    tupleAllocated += tupleAllocatedKB.get(name);
                tupleAllocatedKB.put(name, tupleAllocated);

                long tupleData = t.getLong(9);
                if (tupleKB.containsKey(name))
                    tupleData += tupleKB.get(name);
                tupleKB.put(name, tupleData);

                long stringData = t.getLong(10);
                if (stringKB.containsKey(name))
                    stringData += stringKB.get(name);
                stringKB.put(name,stringData);
            }
        }

    }

    public class StatisticsINDEX {

        public Map<String,Map<String,Long>> tableIndexMaps = new HashMap<String,Map<String,Long>>();


        public StatisticsINDEX(Client client) throws Exception {

            VoltTable t = client.callProcedure("@Statistics","INDEX",0).getResults()[0];
            while (t.advanceRow()) {

                String indexName = t.getString(5);
                String tableName = t.getString(6);
                long kBytes = t.getLong(11);

                // get the map for the tableName (a new empty one, if it doesn't exist
                Map<String,Long> indexSizeMap;
                if (tableIndexMaps.containsKey(tableName)) {
                    indexSizeMap = tableIndexMaps.get(tableName);
                } else {
                    indexSizeMap = new HashMap<String,Long>();
                    tableIndexMaps.put(tableName,indexSizeMap);
                }

                // Add any existing KB to the new KB
                if (indexSizeMap.containsKey(indexName)) {
                    kBytes += indexSizeMap.get(indexName);
                }

                // put the KB into the Map for this index
                indexSizeMap.put(indexName,kBytes);
            }
        }
    }


    public VoltDBMemoryReport(String hostname, String table) throws Exception {
        this.table = table.toUpperCase();
        client = ClientFactory.createClient();
        client.createConnection(hostname);
        System.out.println("Connected to " + hostname);

        statsTable = new StatisticsTABLE(client);
        if (includeIndex) {
            statsIndex = new StatisticsINDEX(client);
        }

    }

    public void run() throws Exception {

        VoltTable columns = client.callProcedure("@SystemCatalog","COLUMNS").getResults()[0];

        // iterate through each table
        VoltTable tables = client.callProcedure("@SystemCatalog","TABLES").getResults()[0];
        while (tables.advanceRow()) {

            String tableType = tables.getString(3);
            String tableName = tables.getString(2);

            // skip export tables
            if (tableType.equals("EXPORT"))
                continue;

            // if specific table requested, skip all other tables
            if (!table.equals("ALL") && !table.equals(tableName))
                continue;

            // is the table replicated?
            long logicalTuples = client.callProcedure("@AdHoc","SELECT COUNT(*) FROM "+tableName+";").getResults()[0].asScalarLong();
            long physicalTuples = statsTable.tupleCount.get(tableName);
            long tupleAllocatedKB = statsTable.tupleAllocatedKB.get(tableName);
            long tupleKB = statsTable.tupleKB.get(tableName);
            long stringKB = statsTable.stringKB.get(tableName);

            String replicatedLabel = " (PARTITIONED)";
            String remarks = tables.getString(4);
            if (tables.wasNull()) {
                replicatedLabel = " (REPLICATED)";
            }

            // print header
            System.out.println();
            System.out.println(tableType + ": " + tableName + replicatedLabel);
            System.out.println(LINE_THICK);
            System.out.printf("  %-30s  %-15s  %15s %15s %15s\n","Column Name","Datatype","(bytes) Min","Actual","Max");
            System.out.printf("  %-30s  %-15s  %15s %15s %15s\n","-----------","--------","---","------","---");
            //System.out.println("Tuples: " + logicalTuples + " actual, " + statsTable.tupleCount.get(tableName) + " copies");



            long rowMin = 0;
            long rowMax = 0;
            long rowAverage = 0;

            // iterate throuch columns
            columns.resetRowPosition();
            while (columns.advanceRow()) {
                // skip this row if it's not in the current table
                String columnTable = columns.getString(2);
                if (!columnTable.equals(tableName))
                    continue;

                String colName = columns.getString(3);
                String typeName = columns.getString(5);
                long nominalLength = columns.getLong(15);
                long octetLength = nominalLength;
                if (typeName.equals("VARCHAR")) {
                    // octet_length column is in characters, not bytes
                    octetLength = nominalLength*4;
                }
                long nullable = columns.getLong(10);

                long fixedSize = 0;
                long minVarSize = 0;
                long maxVarSize = 0;
                long avgVarSize = 0;
                long sampleSize = 0;

                if (typeName.equals("TINYINT")) {
                    fixedSize+=1;
                } else if (typeName.equals("SMALLINT")) {
                    fixedSize+=2;
                } else if (typeName.equals("INTEGER")) {
                    fixedSize+=4;
                } else if (typeName.equals("BIGINT") || typeName.equals("FLOAT") || typeName.equals("TIMESTAMP") ) {
                    fixedSize+=8;
                } else if (typeName.equals("DECIMAL")) {
                    fixedSize+=16;
                } else if (typeName.equals("VARCHAR") || typeName.equals("VARBINARY")) {
                    if (octetLength < 64) {
                        fixedSize+=(octetLength + 1);
                    } else {
                        // min
                        if (nullable == 1) {
                            minVarSize = calcVarSize(null);
                        } else {
                            minVarSize = calcVarSize(0l);
                        }
                        // max
                        maxVarSize+=calcVarSize(octetLength);

                        // actual
                        //String query = "SELECT OCTET_LENGTH(" + colName + "), COUNT(*) FROM " + tableName + " GROUP BY OCTET_LENGTH(" + colName + ");";
                        String query = "SELECT length, COUNT(*) FROM (SELECT OCTET_LENGTH(" + colName + ") as length FROM " + tableName + " LIMIT " + SAMPLE_SIZE + ") a GROUP BY length;";
                        VoltTable vt = client.callProcedure("@AdHoc",query).getResults()[0];
                        while (vt.advanceRow()) {
                            long length = vt.getLong(0);
                            long count = vt.getLong(1);
                            long bytes = calcVarSize(length);
                            avgVarSize += (bytes * count);
                            sampleSize += count;
                        }
                    }
                }

                // combine fixed and variable size
                long minSize = fixedSize + minVarSize;
                long actualSize = fixedSize;
                if (logicalTuples == 0 || sampleSize == 0) {
                    actualSize += minVarSize;
                } else {
                    actualSize += avgVarSize/sampleSize;
                }
                long maxSize = fixedSize + maxVarSize;

                // add this column's values to the totals for the table
                rowMin += minSize;
                rowMax += maxSize;
                rowAverage += actualSize;

                // print stats for the column
                if (typeName.equals("VARCHAR") || typeName.equals("VARBINARY")) {
                    typeName += "("+nominalLength+")";
                }
                //System.out.println("  " + colName + "  " + typeName + "  " + minSize + " min, " + actualSize + " actual, " + maxSize + " max");
                System.out.printf("  %-30s  %-15s  %15s %15s %15s\n",colName,typeName,minSize,actualSize,maxSize);
            }
            // print summary for the table
            System.out.println(LINE);
            System.out.printf("  %-30s  %-15s  %15s %15s %15s\n","Per Row","",rowMin,rowAverage,rowMax);
            System.out.println();
            System.out.printf("  %-30s  %15s  %15s %15s %15s\n","Total Size","Row Count","(MB) Min","Actual","Max");
            System.out.printf("  %-30s  %15s  %15s %15s %15s\n","----------","---------","---","------","---");
            System.out.printf("  %-30s  %15d  %15.2f %15.2f %15.2f\n","Logical",logicalTuples,asMB(rowMin*logicalTuples),asMB(rowAverage*logicalTuples),asMB(rowMax*logicalTuples));
            System.out.printf("  %-30s  %15s  %15.2f %15.2f %15.2f\n","Physical",physicalTuples,asMB(rowMin*physicalTuples),asMB(rowAverage*physicalTuples),asMB(rowMax*physicalTuples));
            System.out.println();
            System.out.printf("  %-60s     %15.2f\n","@Statistics",asMB((tupleKB + stringKB)*1024));
            System.out.printf("  %-60s     %15.2f\n","@Statistics (allocated)",asMB((tupleAllocatedKB + stringKB)*1024));
            System.out.println();

            if (includeIndex) {
                long indexBytes = 0;
                if (statsIndex.tableIndexMaps.containsKey(tableName)) {
                    System.out.println("INDEXES (bytes):");
                    Map<String,Long> indexMap = statsIndex.tableIndexMaps.get(tableName);
                    for (String indexName : indexMap.keySet()) {
                        indexBytes += indexMap.get(indexName)*1024;
                        System.out.println("  " + indexName + ": " + (indexMap.get(indexName)*1024));
                    }
                    System.out.println("  TOTAL: " + indexBytes);
                }
            }

        }

    }

    public static void main(String[] args) throws Exception {

        // default to localhost, if not specified
        String host = "localhost";
        String table = "ALL";
        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            table = args[1];
        }

        VoltDBMemoryReport report = new VoltDBMemoryReport(host,table);

        report.run();
    }
}
