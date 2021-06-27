import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {

    private String tableName;
    private Vector<Page> pages;
    private Vector<Index> indices;
    private int pagesCounter;
    private int indexCounter;
    private final DiskHandler diskHandler = new DiskHandler();

    public Table(String tableName) {
        this.tableName = tableName;
        this.pages = new Vector<>();
        this.indices = new Vector<>();
    }

    public int getIndexCounter() {
        return indexCounter;
    }

    public void setIndexCounter(int indexCounter) {
        this.indexCounter = indexCounter;
    }

    public Vector<Index> getIndices() {
        return indices;
    }

    public Page getPageByPath(String path) {
        for (Page p : pages)
            if (p.getPath().equals(path))
                return p;
        return null;
    }

    public String getTableName() {
        return tableName;
    }

    public Vector<Page> getPages() {
        return pages;
    }

    public int getPagesCounter() {
        return pagesCounter;
    }

    public void setPagesCounter(int pagesCounter) {
        this.pagesCounter = pagesCounter;
    }

    public String createAndSerializePage(Hashtable<String, Object> colNameValue, String primaryKey, int index) throws IOException {

        String pagePath = "src/main/resources/data/Tables/" + getTableName() + "/page" + getPagesCounter() + ".ser";
        setPagesCounter(getPagesCounter() + 1);
        Page newPage = new Page(pagePath);

        Vector<Hashtable<String, Object>> pageRecords = new Vector<>();
        pageRecords.add(colNameValue);
        newPage.setMinClusteringValue(colNameValue.get(primaryKey));
        newPage.setMaxClusteringValue(colNameValue.get(primaryKey));
        newPage.setNumOfRecords(newPage.getNumOfRecords() + 1);

        getPages().add(index, newPage);
        diskHandler.serializeObject(pageRecords, pagePath);
        return pagePath;

    }

    public String createNewPageAndShift(Hashtable<String, Object> colNameValue, Vector<Hashtable<String, Object>> pageRecords, String primaryKey, int idxOfPreviousPage) throws IOException, ClassNotFoundException {

        String newPagePath = "src/main/resources/data/Tables/" + getTableName() + "/page" + getPagesCounter() + ".ser";
        setPagesCounter(getPagesCounter() + 1);

        Vector<Hashtable<String, Object>> newPageRecords = new Vector<>();
        newPageRecords.add(colNameValue);
        for (int i = 0; i < pageRecords.size(); i++) {
            Hashtable<String, Object> record = pageRecords.get(i);
            if (DBApp.compare(record.get(primaryKey), colNameValue.get(primaryKey)) > 0) {
                Hashtable<String, Object> newRecord = pageRecords.remove(i--);
                newPageRecords.add(newRecord);
                HashSet<Object> deleted = new HashSet<>();
                deleted.add(colNameValue.get(primaryKey));
                deleteFromIndex(deleted, colNameValue);
            }
        }
        Page newPage = new Page(newPagePath);

        newPage.setMinClusteringValue(colNameValue.get(primaryKey));
        newPage.setMaxClusteringValue(newPageRecords.get(newPageRecords.size() - 1).get(primaryKey));
        newPage.setNumOfRecords(newPageRecords.size());

        getPages().get(idxOfPreviousPage).setNumOfRecords(pageRecords.size());
        getPages().get(idxOfPreviousPage).setMinClusteringValue(pageRecords.get(0).get(primaryKey));
        getPages().get(idxOfPreviousPage).setMaxClusteringValue(pageRecords.get(pageRecords.size() - 1).get(primaryKey));

        diskHandler.delete(getPages().get(idxOfPreviousPage).getPath());
        diskHandler.serializeObject(pageRecords, getPages().get(idxOfPreviousPage).getPath());

        diskHandler.serializeObject(newPageRecords, newPagePath);
        getPages().add(idxOfPreviousPage + 1, newPage);

        return newPagePath;

    }

    public int searchForPage(Object clusteringObject) {
        int lo = 0;
        int hi = getPages().size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (DBApp.compare(clusteringObject, getPages().get(mid).getMinClusteringValue()) >= 0
                    && DBApp.compare(clusteringObject, getPages().get(mid).getMaxClusteringValue()) <= 0) {
                return mid;
            } else if (DBApp.compare(clusteringObject, getPages().get(mid).getMinClusteringValue()) <= 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return hi + 1 + getPages().size();
    }

    public void deleteFromTableLinear(Hashtable<String, Object> columnNameValue, String clusteringKey, HashSet<Object> deletedRows) throws IOException, ClassNotFoundException {
        Vector<Page> pages = getPages();
        Iterator<Page> pagesIterator = pages.iterator();
        while (pagesIterator.hasNext()) {
            Page p = pagesIterator.next();
            int state = p.deleteFromPage(columnNameValue, clusteringKey, deletedRows);//returns -1 if the page becomes empty
            //update the page's file in disk
            if (state == -1)
                //if the page is empty , dont save the page to disk and remove it from the vector
                pagesIterator.remove();
        }
    }

    public void deleteUsingIndex(Index index, Object grid, Hashtable<String, Object> columnNameValue, String clusteringKey, HashSet<Object> deletedRows) throws IOException, ClassNotFoundException {
        Hashtable<String, Range> colNameRange = index.getColNameRange(columnNameValue);
        Vector<Vector<Bucket>> cells = index.searchInsideIndex(grid, colNameRange);
        Hashtable<String, Vector<Object>> pages = new Hashtable<>();
        for (Vector<Bucket> cell : cells) {
            for (Bucket bucket : cell) {
                Hashtable<Hashtable<String, Object>, Vector<RowReference>> references = (Hashtable<Hashtable<String, Object>, Vector<RowReference>>) diskHandler.deserializeObject(bucket.getPath());
                for (Hashtable<String, Object> compressedRow : references.keySet()) {
                    boolean searchInsidePage = true;
                    for (String column : columnNameValue.keySet()) {
                        searchInsidePage &= compressedRow.containsKey(column) && DBApp.compare(columnNameValue.get(column), compressedRow.get(column)) == 0;
                    }
                    if (searchInsidePage) {
                        Vector<RowReference> rowReferences = references.get(compressedRow);
                        for (RowReference rowReference : rowReferences) {
                            if (!pages.containsKey(rowReference.getPagePath()))
                                pages.put(rowReference.getPagePath(), new Vector<>());
                            pages.get(rowReference.getPagePath()).add(rowReference.getClusteringValue());
                        }
                    }
                }
            }
        }
        for (String pagePath : pages.keySet()) {
            Vector<Object> values = pages.get(pagePath);
            Page curPage = getPageByPath(pagePath);
            for (Object value : values) {
                columnNameValue.put(clusteringKey, value);
                curPage.deleteFromPage(columnNameValue, clusteringKey, deletedRows);
            }
        }
    }

    public void deleteFromIndex(HashSet<Object> deletedRows, Hashtable<String, Object> columnNameValue) throws IOException, ClassNotFoundException {
        Vector<Index> indices = getIndices();
        for (Index index : indices) {
            Object grid = diskHandler.deserializeObject(index.getPath());
            Hashtable<String, Range> colNameRange = index.getColNameRange(columnNameValue);
            Vector<Vector<Bucket>> cells = index.searchInsideIndex(grid, colNameRange);
            for (Vector<Bucket> cell : cells) {
                Iterator<Bucket> bucketIterator = cell.iterator();
                while (bucketIterator.hasNext()) {
                    Bucket bucket = bucketIterator.next();
                    Hashtable<Hashtable<String, Object>, Vector<RowReference>> references = (Hashtable<Hashtable<String, Object>, Vector<RowReference>>) diskHandler.deserializeObject(bucket.getPath());
                    Iterator<Map.Entry<Hashtable<String, Object>, Vector<RowReference>>> iterator = references.entrySet().iterator();
                    int newSize = 0;
                    while (iterator.hasNext()) {
                        Vector<RowReference> rowReferences = iterator.next().getValue();
                        rowReferences.removeIf(rowReference -> deletedRows.contains(rowReference.getClusteringValue()));
                        newSize += rowReferences.size();
                        if (rowReferences.size() == 0)
                            iterator.remove();
                    }
                    diskHandler.delete(bucket.getPath());
                    if (references.isEmpty())
                        bucketIterator.remove();
                    else {
                        bucket.setNumOfRecords(newSize);
                        diskHandler.serializeObject(references, bucket.getPath());
                    }
                }
            }
            diskHandler.delete(index.getPath());
            diskHandler.serializeObject(grid, index.getPath());
        }
    }

    /**
     * searches for the rows that match the entries using linear search and deletes them.
     *
     * @param columnNameValue the entries to which rows will be compared with
     * @param clusteringKey   the clustering key of the table
     * @return true if any rows are deleted.
     * @throws ClassNotFoundException If an error occurred in the stored table pages format
     * @throws IOException            If an I/O error occurred
     */
    public boolean deleteFromTable(Hashtable<String, Object> columnNameValue, String clusteringKey) throws IOException, ClassNotFoundException {
        Vector<Index> indices = getIndices();
        int maximumMatch = 0;
        int maximumMatchIndex = -1;
        for (int i = 0; i < indices.size(); i++) {
            int curMatch = 0;
            for (String columnName : indices.get(i).getColumnNames()) {
                if (!columnNameValue.containsKey(columnName))
                    break;
                curMatch++;
            }
            if (curMatch >= maximumMatch) {
                maximumMatch = curMatch;
                maximumMatchIndex = i;
            }
        }
        HashSet<Object> deletedRows = new HashSet<>();
        if (maximumMatch == 0) {
            deleteFromTableLinear(columnNameValue, clusteringKey, deletedRows);
        } else {
            Index index = indices.get(maximumMatchIndex);
            Object grid = diskHandler.deserializeObject(index.getPath());
            deleteUsingIndex(index, grid, columnNameValue, clusteringKey, deletedRows);
        }
        deleteFromIndex( deletedRows, columnNameValue);
        return deletedRows.size() > 0;
    }

    /**
     * searches for the row that contains the specified clustering value and deletes it.
     *
     * @param columnNameValue the entries to which records will be compared with
     * @param clusteringKey   the clustering key of the table
     * @return true if the row is deleted.
     * @throws ClassNotFoundException If an error occurred in the stored table pages format
     * @throws IOException            If an I/O error occurred
     * @throws DBAppException         If an an error occurred in the table(table not found,types don't match,...)
     */
    public boolean deleteFromTableBinarySearch(Hashtable<String, Object> columnNameValue, String clusteringKey) throws IOException, ClassNotFoundException {
        Vector<Page> pages = getPages();
        int index = searchForPage(columnNameValue.get(clusteringKey));//binary search for the index of the page that contains the clustering value
        if (index >= pages.size())//row not found
            return false;
        Page page = pages.get(index);//page that contains the row to delete
        HashSet<Object> deletedRows = new HashSet<>();
        int state = page.deleteFromPage(columnNameValue, clusteringKey, deletedRows);//returns -1 if the page is empty
        if (!deletedRows.isEmpty())
            deleteFromIndex( deletedRows, columnNameValue);
        if (state == -1) {
            //if the page is empty,remove it from the vector
            pages.remove(index);
            return true;
        }
        return state == 1;
    }

    static String validateExistingTable(String targetTableName) throws IOException, DBAppException {
        BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        String curLine;
        while (br.ready()) {
            curLine = br.readLine();
            String[] metaData = curLine.split(",");
            if (metaData[0].equals(targetTableName)) {
                if (metaData[3].equals("True")) {
                    return metaData[1];
                }
            }
        }
        throw new DBAppException("There is no such table in the database.");
    }

    static Object[] getTableInfo(String tableName) throws IOException, ParseException, DBAppException {
        validateExistingTable(tableName);
        FileReader metadata = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(metadata);
        String curLine;
        Hashtable<String, String> colDataTypes = new Hashtable<>();
        Hashtable<String, Object> colMin = new Hashtable<>();
        Hashtable<String, Object> colMax = new Hashtable<>();
        String clusteringType = "", clusteringCol = "";
        while ((curLine = br.readLine()) != null) {
            String[] res = curLine.split(",");
            if (res[0].equals(tableName)) {
                colDataTypes.put(res[1], res[2]);
                switch (res[2]) {
                    case "java.lang.Integer":
                        colMin.put(res[1], Integer.parseInt(res[5]));
                        colMax.put(res[1], Integer.parseInt(res[6]));
                        break;
                    case "java.lang.Double":
                        colMin.put(res[1], Double.parseDouble(res[5]));
                        colMax.put(res[1], Double.parseDouble(res[6]));
                        break;
                    case "java.util.Date":
                        colMin.put(res[1], new SimpleDateFormat("yyyy-MM-dd").parse(res[5]));
                        colMax.put(res[1], new SimpleDateFormat("yyyy-MM-dd").parse(res[6]));
                        break;
                    default:
                        colMin.put(res[1], res[5]);
                        colMax.put(res[1], res[6]);
                        break;
                }
                if (res[3].equals("True")) {
                    clusteringType = res[2];
                    clusteringCol = res[1];
                }
            }
        }
        return new Object[]{colDataTypes, colMin, colMax, clusteringType, clusteringCol};
    }

    /**
     * Returns an array containing useful info about the passed table in the parameter.
     *
     * @param tableName name of the table that the method will use to retrieve data from metadata.csv
     * @return Array of objects.
     * <ul>
     *     <li>array[0] a Hashtable containing every column in the table and its data type.</li>
     *     <li>array[1] a Hashtable containing every column in the table and its allowed minimum value.</li>
     *     <li>array[2] a Hashtable containing every column in the table and its allowed maximum value.</li>
     *     <li>array[3] a String containing the data type of the clustering column</li>
     *     <li>array[4] a String containing the name of the clustering column</li>
     * </ul>
     * @throws IOException
     * @throws ParseException
     */
    static Object[] validateUpdateInput(String tableName, String clusteringKeyValue,
                                         Hashtable<String, Object> columnNameValue) throws IOException, ParseException, DBAppException, ClassNotFoundException {
        Object[] tableInfo = getTableInfo(tableName);

        Hashtable<String, String> colDataTypes = (Hashtable<String, String>) tableInfo[0];
        Hashtable<String, Object> colMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> colMax = (Hashtable<String, Object>) tableInfo[2];
        String clusteringType = (String) tableInfo[3], clusteringCol = (String) tableInfo[4];

        Object clusteringObject;
        switch (clusteringType) {
            case "java.lang.Integer":
                try {
                    clusteringObject = Integer.parseInt(clusteringKeyValue);
                } catch (NumberFormatException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            case "java.lang.Double":
                try {
                    clusteringObject = Double.parseDouble(clusteringKeyValue);
                } catch (NumberFormatException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            case "java.util.Date":
                try {
                    clusteringObject = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
                } catch (ParseException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            default:
                clusteringObject = clusteringKeyValue;
                break;
        }

        checkColumnsCompatibility(columnNameValue, colDataTypes);
        checkValuesRanges(columnNameValue, colDataTypes, colMin, colMax);
        if (columnNameValue.get(clusteringCol) != null) {
            throw new DBAppException("Cannot update clustering column.");
        }
        return new Object[]{clusteringCol, clusteringObject};
    }

    private static void checkColumnsCompatibility(Hashtable<String, Object> columnNameValue, Hashtable<String, String> colDataTypes) throws DBAppException, ClassNotFoundException {
        for (String key : columnNameValue.keySet()) {
            if (!colDataTypes.containsKey(key)) {
                throw new DBAppException("Column does not exist");
            }
            Class colClass = Class.forName(colDataTypes.get(key));
            if (!colClass.isInstance(columnNameValue.get(key))) {
                throw new DBAppException("Incompatible data types");
            }
        }
    }

    private static void checkValuesRanges(Hashtable<String, Object> columnNameValue, Hashtable<String, String> colDataTypes, Hashtable<String, Object> colMin, Hashtable<String, Object> colMax) throws DBAppException {
        for (String key : columnNameValue.keySet()) {
            switch (colDataTypes.get(key)) {
                case "java.lang.Integer":
                    if (((Integer) columnNameValue.get(key)).compareTo((Integer) colMin.get(key)) < 0) {
                        throw new DBAppException("Value for column " + key + " is below the minimum allowed");
                    }
                    if (((Integer) columnNameValue.get(key)).compareTo((Integer) colMax.get(key)) > 0) {
                        throw new DBAppException("Value for column " + key + " is above the maximum allowed");
                    }
                    break;
                case "java.lang.Double":
                    if (((Double) columnNameValue.get(key)).compareTo((Double) colMin.get(key)) < 0) {
                        throw new DBAppException("Value for column " + key + " is below the minimum allowed");
                    }
                    if (((Double) columnNameValue.get(key)).compareTo((Double) colMax.get(key)) > 0) {
                        throw new DBAppException("Value for column " + key + " is above the maximum allowed");
                    }
                    break;
                case "java.util.Date":
                    if (((Date) columnNameValue.get(key)).compareTo((Date) colMin.get(key)) < 0) {
                        throw new DBAppException("Value for column " + key + " is below the minimum allowed");
                    }
                    if (((Date) columnNameValue.get(key)).compareTo((Date) colMax.get(key)) > 0) {
                        throw new DBAppException("Value for column " + key + " is above the maximum allowed");
                    }
                    break;
                default:
                    if (((String) columnNameValue.get(key)).compareTo((String) colMin.get(key)) < 0) {
                        throw new DBAppException("Value for column " + key + " is below the minimum allowed. Min: " + colMin.get(key) + ". Found: " + columnNameValue.get(key));
                    }
                    if (((String) columnNameValue.get(key)).compareTo((String) colMax.get(key)) > 0) {
                        throw new DBAppException("Value for column " + key + " is above the maximum allowed. Max: " + colMax.get(key) + ". Found: " + columnNameValue.get(key));
                    }
                    break;
            }
        }
    }

    /**
     * validates the input values by checking that the entries' values are in the range and their types are compatible.
     *
     * @param tableName       the name of the table to get the max,min and data types from.
     * @param columnNameValue the entries to validate.
     * @return the name of the clustering key column
     * @throws ClassNotFoundException If an error occurred in the stored table pages format
     * @throws IOException            If an I/O error occurred
     * @throws DBAppException         If an an error occurred in the table(No rows are deleted,table not found,types don't match,...)
     * @throws ParseException         If an error occurred while parsing the input
     */
    static String validateDeleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws IOException, ParseException, DBAppException, ClassNotFoundException {
        Object[] tableInfo = Table.getTableInfo(tableName);
        Hashtable<String, String> colDataTypes = (Hashtable<String, String>) tableInfo[0];
        Hashtable<String, Object> colMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> colMax = (Hashtable<String, Object>) tableInfo[2];
        String clusteringCol = (String) tableInfo[4];
        checkColumnsCompatibility(columnNameValue, colDataTypes);
        checkValuesRanges(columnNameValue, colDataTypes, colMin, colMax);
        return clusteringCol;
    }

    static Vector<Hashtable<String, Object>> rowsUnion(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b, String clusteringColumnName) {
        Vector<Hashtable<String, Object>> resultOfUnion = new Vector<>();
        resultOfUnion.addAll(a);
        resultOfUnion.addAll(rowsDifference(b, a, clusteringColumnName));
        return resultOfUnion;
    }

    static Vector<Hashtable<String, Object>> rowsDifference(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b, String clusteringColumnName) {
        Vector<Hashtable<String, Object>> resultOfDifference = new Vector<>();
        for (Hashtable<String, Object> rowA : a) {
            boolean found = false;
            for (Hashtable<String, Object> rowB : b) {
                if (rowA.get(clusteringColumnName).equals(rowB.get(clusteringColumnName))) {
                    found = true;
                }
            }
            if (!found)
                resultOfDifference.add(rowA);
        }
        return resultOfDifference;
    }

    static Vector<Hashtable<String, Object>> rowsIntersection(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b, String clusteringColumnName) {
        Vector<Hashtable<String, Object>> resultOfIntersection = new Vector<>();
        for (Hashtable<String, Object> rowA : a) {
            boolean found = false;
            for (Hashtable<String, Object> rowB : b) {
                if (rowA.get(clusteringColumnName).equals(rowB.get(clusteringColumnName))) {
                    found = true;
                    break;
                }
            }
            if (found)
                resultOfIntersection.add(rowA);
        }
        return resultOfIntersection;
    }

    static String validateRecord(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        //complete.
        //The method checks for the following
        //The input record include the primary key.
        //The input record's values are of the right types as the table in the metadata.
        //The input record's values are in the range between their min and max values.

        boolean found = false;
        ArrayList<String[]> tableCols = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line;
            big:
            while ((line = br.readLine()) != null) {
                String[] record = line.split(",");
                if (record[0].equals(tableName)) {
                    found = true;
                    tableCols.add(record);
                    while ((line = br.readLine()) != null) {
                        record = line.split(",");
                        if (record[0].equals(tableName)) {
                            tableCols.add(record);
                        } else break big;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!found) {
            throw new DBAppException("There is no such table in the Database.");
        }

        boolean primaryKeyExist = false;
        String primaryKey = null;
        for (String[] record : tableCols) {
            boolean valid = true;
            if (record[3].equals("True")) {
                primaryKeyExist = colNameValue.get(record[1]) != null;
                primaryKey = record[1];
            }
            if (!colNameValue.containsKey(record[1]))
                continue;
            Class c = colNameValue.get(record[1]).getClass();
            if ((c.getName()).equals(record[2])) {
                switch (c.getName()) {
                    case "java.lang.Integer": {
                        Integer min = Integer.parseInt(record[5]);
                        Integer max = Integer.parseInt(record[6]);
                        if (!(((Integer) colNameValue.get(record[1])).compareTo(min) >= 0 && ((Integer) colNameValue.get(record[1])).compareTo(max) <= 0))
                            valid = false;
                        break;
                    }
                    case "java.lang.String": {
                        String min = record[5];
                        String max = record[6];
                        if (!(((String) colNameValue.get(record[1])).compareTo(min) >= 0 && ((String) colNameValue.get(record[1])).compareTo(max) <= 0))
                            valid = false;
                        break;
                    }
                    case "java.lang.Double": {
                        double min = Double.parseDouble(record[5]);
                        double max = Double.parseDouble(record[6]);
                        if (!((double) colNameValue.get(record[1]) >= min && (double) colNameValue.get(record[1]) <= max))
                            valid = false;
                        break;
                    }
                    default:
                        try {
                            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                            Date min = format.parse(record[5]);
                            Date max = format.parse(record[6]);
                            if (((Date) colNameValue.get(record[1])).compareTo(min) < 0 || ((Date) colNameValue.get(record[1])).compareTo(max) > 0)
                                valid = false;
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        break;
                }
                if (!valid)
                    throw new DBAppException("The value of the field " + record[1] + " is not in the range.\nThe value should be between " + record[5] + " and " + record[6] + ". Found: " + colNameValue.get(record[1]));
            } else
                throw new DBAppException("The value of the field " + record[1] + " is incompatible.\nThe value should be an instance of " + record[2] + ".");
        }
        if (!primaryKeyExist)
            throw new DBAppException("The primary key must be included in the record.");
        return primaryKey;
    }


}
