import java.io.*;
import java.util.*;

public class Index implements Serializable {
    private String path;
    private final String[] columnNames;
    private final Range[][] columnRanges;
    private final int columnsCount;
    private int numOfBuckets;
    private final static DiskHandler diskHandler = new DiskHandler();

    public int getNumOfBuckets() {
        return numOfBuckets;
    }

    public void setNumOfBuckets(int numOfBuckets) {
        this.numOfBuckets = numOfBuckets;
    }

    public Index(String path, String[] columnNames, Hashtable<String, Object> minValPerCol, Hashtable<String, Object> maxValPerCol) {
        this.columnsCount = columnNames.length;
        this.path = path;
        this.columnNames = columnNames;
        columnRanges = new Range[columnsCount][10];
        for (int i = 0; i < columnsCount; i++)
            columnRanges[i] = buildRanges(minValPerCol.get(columnNames[i]), maxValPerCol.get(columnNames[i]));
    }

    public int getColumnsCount() {
        return columnsCount;
    }

    private Range[] buildRanges(Object minVal, Object maxVal) {
        if (minVal instanceof String)
            return buildStringRanges((String) minVal);
        return buildNumbersRanges(minVal, maxVal);
    }

    public static void main(String[] args) {
//        Hashtable<String, Object> max = new Hashtable<>();
//        Hashtable<String, Object> min = new Hashtable<>();
//        max.put("employer", "a");
//        min.put("employer", "aa");
//        min.put("ID", new Date());
//        max.put("ID", new Date(((Date) min.get("ID")).getTime() + 3000));
//        System.out.println(max.get("ID"));
//        Index index = new Index("", new String[]{"employer", "ID"}, min, max);
//        System.out.println(index.getPosition(new Date(new Date().getTime() + 2900), 1));
//        System.out.println(Arrays.toString(index.columnRanges[0]) + "\n" + Arrays.toString(index.columnRanges[1]));

    }

    private Range[] buildNumbersRanges(Object minVal, Object maxVal) {
        Object difference = getDifference(minVal, maxVal);
        Range[] ranges = new Range[10];
        for (int i = 0; i < 10; i++) {
            if (i == 0) {
                ranges[i] = new Range((Comparable) minVal, add(minVal, difference));
            } else {
                Comparable min = ranges[i - 1].getMaxVal();
                Comparable max = add(min, difference);
                ranges[i] = new Range(min, max);
            }
        }
        return ranges;
    }

    private Comparable add(Object value, Object toAdd) {
        if (value instanceof Integer)
            return (int) value + (int) toAdd;
        if (value instanceof Double)
            return (double) value + (double) toAdd;
        return new Date(((Date) value).getTime() + (long) toAdd);
    }

    private Object getDifference(Object minVal, Object maxVal) {
        if (minVal instanceof Integer)
            return (((Integer) maxVal - (Integer) minVal) + 9) / 10;
        if (minVal instanceof Double)
            return ((Double) maxVal - (Double) minVal + 1) / 10.0;
        return (((Date) maxVal).getTime() - ((Date) minVal).getTime() + 9) / 10;
    }

    private static Range[] buildStringRanges(String minValue) {
        Range[] ranges = new Range[10];
        for (int i = 0; i < 10; i++) {
            if (Character.isAlphabetic(minValue.charAt(0))) {
                if (i == 0) {
                    ranges[i] = new Range('a', 'd');
                } else {
                    char min = (char) ranges[i - 1].getMaxVal();
                    char max = (char) (min + (i < 6 ? 3 : i < 9 ? 2 : 1));
                    ranges[i] = new Range(min, max);
                }
            } else {
                ranges[i] = i < 9 ? new Range((char) (i + '0'), (char) (i + 1 + '0')) : new Range((char) (i + '0'), (char) (i + '0'));
            }
        }
        return ranges;
    }


    public Vector<Integer> getPosition(Range r, int column) {
        Vector<Integer> ans = new Vector<>();
        if (r.getMinVal() instanceof String) {
            r.setMinVal(((String) r.getMinVal()).toLowerCase());
            r.setMaxVal(((String) r.getMaxVal()).toLowerCase());
            String min= (String) r.getMinVal();
            String max=(String) r.getMaxVal();
            r.setMinVal(min.charAt(0));
            r.setMaxVal(max.charAt(0));
        }
        for (int i = 0; i < 10; i++)
            if (i == 9) {
                if (isInRangeIncInc(columnRanges[column][i], r)) {
                    ans.add(i);
                }
            } else {
                if (isInRangeIncExc(columnRanges[column][i], r))
                    ans.add(i);
            }
        return ans;
    }

    private boolean isInRangeIncExc(Range r1, Range r2) {
        return !(r1.getMaxVal().compareTo(r2.getMinVal()) <= 0 || r1.getMinVal().compareTo(r2.getMaxVal()) > 0);
    }

    private boolean isInRangeIncInc(Range r1, Range r2) {
        return !(r1.getMaxVal().compareTo(r2.getMinVal()) < 0 || r1.getMinVal().compareTo(r2.getMaxVal()) > 0);
    }

    public String getPath() {
        return path;
    }

    public String[] getColumnNames() {
        return columnNames;
    }


    public boolean isSameIndex(Index index) {
        if (index.getColumnNames().length != this.columnNames.length)
            return false;

        for (int i = 0; i < index.getColumnNames().length; i++)
            if (!index.getColumnNames()[i].equals(this.columnNames[i]))
                return false;

        return true;
    }

    static void insertIntoIndex(Vector<Index> indices, Vector<Hashtable<String, Object>> rows, String primaryKey, String pagePath) throws IOException, ClassNotFoundException {
        for (Index index : indices) {
            Object grid = diskHandler.deserializeObject(index.getPath());
            for (Hashtable<String, Object> row : rows) {
                String[] colNames = index.getColumnNames();
                Hashtable<String, Object> keyHashTable = new Hashtable<>();
                Hashtable<String, Range> keySearchHashTable = new Hashtable<>();
                //keyHashTable is the key to use to search inside the bucket
                for (String colName : colNames) {
                    if (row.containsKey(colName)) {
                        keyHashTable.put(colName, row.get(colName));
                        Comparable object = (Comparable) row.get(colName);
                        keySearchHashTable.put(colName, new Range(object, object));
                    }
                }
                //Now we search for the bucket inside the index
                Vector<Vector<Bucket>> cellsVector = index.searchInsideIndex(grid, keySearchHashTable);
                for (Vector<Bucket> bucketVector : cellsVector) {
                    boolean foundBucket = false;
                    String indexPath = index.getPath();
                    Bucket bucket = new Bucket(indexPath.substring(0, indexPath.length() - 4) + "/bucket" + index.getNumOfBuckets() + ".ser");
                    index.setNumOfBuckets(index.getNumOfBuckets() + 1);

                    for (Bucket b : bucketVector) {
                        if (b.getNumOfRecords() < DBApp.readConfig()[1]) {
                            foundBucket = true;
                            bucket = b;
                            index.setNumOfBuckets(index.getNumOfBuckets() - 1);
                            break;
                        }
                    }
                    Hashtable<Hashtable<String, Object>, Vector<RowReference>> bucketHashTable;
                    Vector<RowReference> newVector;
                    if (foundBucket) {
                        bucketHashTable = (Hashtable<Hashtable<String, Object>, Vector<RowReference>>) diskHandler.deserializeObject(bucket.getPath());
                        //We can add this record to this bucket
                        if (bucketHashTable.containsKey(keyHashTable)) {
                            newVector = bucketHashTable.get(keyHashTable);
                        } else {
                            //The values of the record was never in the index before
                            newVector = new Vector<>();
                        }
                    } else {
                        bucketVector.add(bucket);
                        bucketHashTable = new Hashtable<>();
                        newVector = new Vector<>();
                    }

                    newVector.add(new RowReference(pagePath, row.get(primaryKey)));
                    bucketHashTable.put(keyHashTable, newVector);
                    bucket.setNumOfRecords(bucket.getNumOfRecords() + 1);

                    //Delete then serialize the the index and the bucket again
                    diskHandler.delete(bucket.getPath());
                    diskHandler.serializeObject(bucketHashTable, bucket.getPath());
                }
                diskHandler.delete(index.getPath());
                diskHandler.serializeObject(grid, index.getPath());
            }
        }
    }

    static void insertIntoIndexByPage(Vector<Index> indices, Page page, String primaryKey) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(page.getPath());
        Index.insertIntoIndex(indices, pageRecords, primaryKey, page.getPath());
    }

    public Vector<Vector<Bucket>> searchInsideIndex(Object grid, Hashtable<String, Range> colNameValue) {
        Vector<Integer>[] indices = new Vector[colNameValue.size()];
        String[] columnNames = getColumnNames();
        for (int i = 0; i < indices.length; i++)
            indices[i] = getPosition(colNameValue.get(columnNames[i]), i);

        Vector<Object> subGrids = new Vector<>();
        getSubGrids(grid, indices, 0, subGrids);
        Vector<Vector<Bucket>> totalBuckets = new Vector<>();
        int level = getColumnsCount() - colNameValue.size();

        for (Object subGrid : subGrids)
            getResultBuckets(subGrid, level, totalBuckets);

        return totalBuckets;
    }

    private void getSubGrids(Object grid, Vector<Integer>[] v, int idx, Vector<Object> subGrids) {
        if (idx == v.length)
            subGrids.add(grid);
        else
            for (int x : v[idx])
                getSubGrids(((Object[]) grid)[x], v, idx + 1, subGrids);
    }

    private void getResultBuckets(Object grid, int curLevel, Vector<Vector<Bucket>> buckets) {
        if (curLevel == 0) {
            buckets.add((Vector<Bucket>) grid);
            return;
        }
        for (int i = 0; i < 10; i++)
            getResultBuckets(((Object[]) grid)[i], curLevel - 1, buckets);
    }

    public void initializeGridCells(Object grid, int curLevel) {
        if (curLevel == 1) {
            for (int i = 0; i < 10; i++)
                ((Object[]) grid)[i] = new Vector<Bucket>();
            return;
        }
        for (int i = 0; i < 10; i++)
            initializeGridCells(((Object[]) grid)[i], curLevel - 1);
    }

    static void updateMetaDataFile(String tableName, String[] indexColumns) throws IOException {

        FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);

        StringBuilder newMetaData = new StringBuilder();
        String curLine = "";

        while ((curLine = br.readLine()) != null) {
            String[] curLineSplit = curLine.split(",");

            if (!curLineSplit[0].equals(tableName)) {
                newMetaData.append(curLine);
                newMetaData.append("\n");
                continue;
            }

            StringBuilder tmpString = new StringBuilder(curLine);

            for (String col : indexColumns) {
                if (col.equals(curLineSplit[1])) {
                    tmpString = new StringBuilder();
                    for (int i = 0; i < curLineSplit.length; i++)
                        if (i == 4)
                            tmpString.append("True,");
                        else if (i == 6)
                            tmpString.append(curLineSplit[i]);
                        else
                            tmpString.append(curLineSplit[i] + ",");
                }
            }
            newMetaData.append(tmpString + "\n");
        }

        FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
        metaDataFile.write(newMetaData.toString());
        metaDataFile.close();
    }

    public Hashtable<String, Range> getColNameRange(Hashtable<String, Object> columnNameValue) {
        Hashtable<String, Range> colNameRange = new Hashtable<>();
        String[] columnNames = getColumnNames();
        for (String columnName : columnNames) {
            if (!columnNameValue.containsKey(columnName))
                break;
            Comparable val = (Comparable) columnNameValue.get(columnName);
            colNameRange.put(columnName, new Range(val, val));

        }
        return colNameRange;
    }

}
