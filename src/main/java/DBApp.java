import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.*;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp implements DBAppInterface {
    private final DiskHandler diskHandler = new DiskHandler();

    static class Pair {
        Index index;
        Vector<SQLTerm> terms;

        public Pair(Index index, Vector<SQLTerm> terms) {
            this.index = index;
            this.terms = terms;
        }

        public Index getIndex() {
            return index;
        }

        public Vector<SQLTerm> getTerms() {
            return terms;
        }

        public void add(SQLTerm term) {
            terms.add(term);
        }
    }


    @Override
    public void init() {
        File tablesDirectory = new File("src/main/resources/data/Tables");
        if (!tablesDirectory.exists())
            tablesDirectory.mkdirs();

        try {
            FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            if (br.readLine() == null) {
                FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
                StringBuilder tableMetaData = new StringBuilder();
                tableMetaData.append("Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max");
                metaDataFile.write(tableMetaData.toString());
                metaDataFile.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {

        //validating input
        for (String col : colNameType.keySet())
            if (!colNameMax.containsKey(col) || !colNameMin.containsKey(col))
                throw new DBAppException();

        for (String col : colNameMax.keySet())
            if (!colNameType.containsKey(col) || !colNameMin.containsKey(col))
                throw new DBAppException();

        for (String col : colNameMin.keySet())
            if (!colNameType.containsKey(col) || !colNameMax.containsKey(col))
                throw new DBAppException();


        File tableDirectory = new File("src/main/resources/data/Tables/" + tableName);
        File indexDirectory = new File("src/main/resources/data/Tables/" + tableName + "/Indices");

        if (tableDirectory.exists())
            throw new DBAppException();
        else
            tableDirectory.mkdir();
        indexDirectory.mkdirs();

        Table tableInstance = new Table(tableName);

        try {
            FileOutputStream serializedFile = new FileOutputStream("src/main/resources/data/Tables/" + tableName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(serializedFile);
            out.writeObject(tableInstance);
            out.close();
            serializedFile.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        //writing table info in MetaData
        try {

            FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");

            BufferedReader br = new BufferedReader(oldMetaDataFile);
            StringBuilder tableMetaData = new StringBuilder();

            String curLine;
            while ((curLine = br.readLine()) != null)
                tableMetaData.append(curLine).append('\n');

            FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
            for (String colName : colNameType.keySet()) {
                tableMetaData.append(tableName).append(",");
                tableMetaData.append(colName).append(",");
                tableMetaData.append(colNameType.get(colName)).append(",");
                tableMetaData.append(colName.equals(clusteringKey) ? "True" : "False").append(",");
                tableMetaData.append("False,");
                tableMetaData.append(colNameMin.get(colName)).append(",");
                tableMetaData.append(colNameMax.get(colName));
                tableMetaData.append("\n");
            }
            metaDataFile.write(tableMetaData.toString());
            metaDataFile.close();
        } catch (IOException ignored) {
        }
    }


    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        String pagePathForIndex;
        //Check the table exists and the input record is valid.
        String primaryKey = Table.validateRecord(tableName, colNameValue);
        Vector<Hashtable<String, Object>> rowsForIndex = new Vector<>();
        try {
            //Deserialize table and get the index of the page to insert in
            Table t = (Table) diskHandler.deserializeObject("src/main/resources/data/Tables/" + tableName + ".ser");
            Vector<Page> pages = t.getPages();
            int idxOfPage = t.searchForPage(colNameValue.get(primaryKey));
            if (pages.size() == 0) {
                //empty table
                pagePathForIndex = t.createAndSerializePage(colNameValue, primaryKey, 0);
                rowsForIndex.add(colNameValue);
                Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);
                diskHandler.delete("src/main/resources/data/Tables/" + tableName + ".ser");
                diskHandler.serializeObject(t, "src/main/resources/data/Tables/" + tableName + ".ser");
                return;
            }
            int maxCountInPage = readConfig()[0];

            if (idxOfPage < pages.size()) {
                //I found the exact page I should insert in
                Page curPage = pages.get(idxOfPage);
                Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(curPage.getPath());
                int idxOfRecord = Page.searchInsidePage(pageRecords, colNameValue.get(primaryKey), primaryKey);
                if (idxOfRecord < pageRecords.size())
                    throw new DBAppException("The record already exists in the table.");
                if (pageRecords.size() == maxCountInPage) {
                    //Page is full. Check the following page.
                    if (idxOfPage + 1 < pages.size()) {
                        Page followingPage = pages.get(idxOfPage + 1);
                        if (followingPage.getNumOfRecords() == maxCountInPage) {
                            //following page is also full. Create new page in between and shift.
                            t.createNewPageAndShift(colNameValue, pageRecords, primaryKey, idxOfPage);
                            Index.insertIntoIndexByPage(t.getIndices(), pages.get(idxOfPage + 1), primaryKey);

                        } else {
                            //following page is not full.
                            //shift last record in the current page to the following page
                            //and insert the record in place in the current page.
                            pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                            rowsForIndex.add(colNameValue);
                            Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);

                            Hashtable<String, Object> lastRecord = pageRecords.remove(pageRecords.size() - 1);
                            Vector<Hashtable<String, Object>> followingPageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(followingPage.getPath());

                            followingPageRecords.add(0, lastRecord);
                            followingPage.setMinClusteringValue(lastRecord.get(primaryKey));
                            followingPage.setNumOfRecords(followingPageRecords.size());

                            curPage.setNumOfRecords(pageRecords.size());
                            curPage.setMaxClusteringValue(pageRecords.get(pageRecords.size() - 1).get(primaryKey));

                            diskHandler.delete(followingPage.getPath());
                            diskHandler.serializeObject(followingPageRecords, followingPage.getPath());

                            diskHandler.delete(curPage.getPath());
                            diskHandler.serializeObject(pageRecords, curPage.getPath());

                            HashSet<Object> deleted = new HashSet<>();
                            deleted.add(lastRecord.get(primaryKey));
                            t.deleteFromIndex(deleted, lastRecord);
                            rowsForIndex.clear();
                            rowsForIndex.add(lastRecord);
                            Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, followingPage.getPath());
                        }
                    } else {
                        //current page was the last page in the table.
                        //need to create new page and shift some records
                        t.createNewPageAndShift(colNameValue, pageRecords, primaryKey, idxOfPage);
                        Index.insertIntoIndexByPage(t.getIndices(), pages.get(idxOfPage + 1), primaryKey);
                    }
                } else {
                    pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                    rowsForIndex.add(colNameValue);
                    Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);
                }
            } else {
                //The key doesn't belong to a range in any page.
                idxOfPage -= pages.size();
                if (idxOfPage == 0) {
                    //check for place in page at index 0 else create new page
                    Page curPage = pages.get(0);
                    if (curPage.getNumOfRecords() < maxCountInPage) {
                        Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(curPage.getPath());
                        int idxOfRecord = Page.searchInsidePage(pageRecords, colNameValue.get(primaryKey), primaryKey);
                        pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                    } else {
                        pagePathForIndex = t.createAndSerializePage(colNameValue, primaryKey, 0);
                    }
                    rowsForIndex.add(colNameValue);
                    Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);
                } else if (idxOfPage == pages.size()) {
                    //check for place in last page else create new page
                    Page curPage = pages.get(pages.size() - 1);
                    if (curPage.getNumOfRecords() < maxCountInPage) {
                        Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(curPage.getPath());
                        int idxOfRecord = Page.searchInsidePage(pageRecords, colNameValue.get(primaryKey), primaryKey);
                        pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                    } else {
                        pagePathForIndex = t.createAndSerializePage(colNameValue, primaryKey, pages.size());
                    }
                    rowsForIndex.add(colNameValue);
                    Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);
                } else {
                    //check for the current page then the following else create a new page
                    Page curPage = pages.get(idxOfPage - 1);
                    if (curPage.getNumOfRecords() < maxCountInPage) {
                        Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(curPage.getPath());
                        int idxOfRecord = Page.searchInsidePage(pageRecords, colNameValue.get(primaryKey), primaryKey);
                        pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                    } else {
                        curPage = pages.get(idxOfPage); //this now is following page
                        if (curPage.getNumOfRecords() < maxCountInPage) {
                            Vector<Hashtable<String, Object>> pageRecords = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(curPage.getPath());
                            int idxOfRecord = Page.searchInsidePage(pageRecords, colNameValue.get(primaryKey), primaryKey);
                            pagePathForIndex = curPage.insertRecordInPlace(idxOfRecord, colNameValue, primaryKey, pageRecords);
                        } else {
                            pagePathForIndex = t.createAndSerializePage(colNameValue, primaryKey, idxOfPage);
                        }
                    }
                    rowsForIndex.add(colNameValue);
                    Index.insertIntoIndex(t.getIndices(), rowsForIndex, primaryKey, pagePathForIndex);
                }
            }
            diskHandler.delete("src/main/resources/data/Tables/" + tableName + ".ser");
            diskHandler.serializeObject(t, "src/main/resources/data/Tables/" + tableName + ".ser");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static int[] readConfig() {
        Properties prop = new Properties();
        String filePath = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        int[] arr = new int[2];
        arr[0] = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        arr[1] = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
        return arr;
    }


    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException, ParseException, ClassNotFoundException {

        String clusteringKey = Table.validateExistingTable(tableName);
        Object[] tableInfo = Table.getTableInfo(tableName);
        Hashtable<String, String> columnsInfo = (Hashtable<String, String>) tableInfo[0];

        for (String colName : columnNames)
            if (!columnsInfo.containsKey(colName))
                throw new DBAppException("No such column exits!");

        Table table = (Table) diskHandler.deserializeObject("src/main/resources/data/Tables/" + tableName + ".ser");

        String indexPath = "src/main/resources/data/Tables/" + tableName + "/Indices/index" + table.getIndexCounter() + ".ser";
        Index newIndex = new Index(indexPath, columnNames, (Hashtable<String, Object>) tableInfo[1], (Hashtable<String, Object>) tableInfo[2]);
        for (Index idx : table.getIndices())
            if (newIndex.isSameIndex(idx))
                throw new DBAppException("Index already exists!");

        Index.updateMetaDataFile(tableName, columnNames);

        table.getIndices().add(newIndex);
        table.setIndexCounter(table.getIndexCounter() + 1);

        int[] dimensions = new int[columnNames.length];
        Arrays.fill(dimensions, 10);
        Object gridIndex = Array.newInstance(Vector.class, dimensions);
        newIndex.initializeGridCells(gridIndex, dimensions.length);
        File indexFolder = new File(indexPath.substring(0, indexPath.length() - 4));
        indexFolder.mkdirs();
        Vector<Page> tablePages = table.getPages();
        Vector<Index> editedIndices = new Vector<>();
        editedIndices.add(newIndex);
        diskHandler.serializeObject(gridIndex, newIndex.getPath());
        for (Page page : tablePages)
            Index.insertIntoIndexByPage(editedIndices, page, clusteringKey);

        diskHandler.delete("src/main/resources/data/Tables/" + tableName + ".ser");
        diskHandler.serializeObject(table, "src/main/resources/data/Tables/" + tableName + ".ser");
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue,
                            Hashtable<String, Object> columnNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        Object[] clusteringInfo = Table.validateUpdateInput(tableName, clusteringKeyValue, columnNameValue);
        String clusteringCol = (String) clusteringInfo[0];
        Object clusteringObject = clusteringInfo[1];

        Table t = (Table) diskHandler.deserializeObject("src/main/resources/data/Tables/" + tableName + ".ser");

        int pageIdx = t.searchForPage(clusteringObject);
        if (pageIdx >= t.getPages().size()) {
            System.out.println("Invalid clustering key value");
            return;
        }
        Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(t.getPages().get(pageIdx).getPath());

        int rowIdx = Page.searchInsidePage(page, clusteringObject, clusteringCol);
        if (rowIdx >= page.size()) {
            System.out.println("Invalid clustering key value");
            return;
        }
        HashSet<Object> deleted = new HashSet<>();
        deleted.add(clusteringObject);
        t.deleteFromIndex( deleted, page.get(rowIdx));
        for (String key : columnNameValue.keySet()) {
            page.get(rowIdx).replace(key, columnNameValue.get(key));
        }
        diskHandler.delete(t.getPages().get(pageIdx).getPath());
        diskHandler.serializeObject(page, t.getPages().get(pageIdx).getPath());
        Vector<Hashtable<String, Object>> insertedIntoIndex = new Vector<>();
        insertedIntoIndex.add(page.get(pageIdx));
        Index.insertIntoIndex(t.getIndices(), insertedIntoIndex, clusteringCol, t.getPages().get(pageIdx).getPath());

    }

    static int compare(Object clusteringObject1, Object clusteringObject2) {
        if (clusteringObject1 instanceof java.lang.Integer) {
            return ((Integer) clusteringObject1).compareTo((Integer) clusteringObject2);
        } else if (clusteringObject1 instanceof java.lang.Double) {
            return ((Double) clusteringObject1).compareTo((Double) clusteringObject2);
        } else if (clusteringObject1 instanceof java.util.Date) {
            return ((Date) clusteringObject1).compareTo((Date) clusteringObject2);
        } else {
            return ((String) clusteringObject1).compareTo((String) clusteringObject2);
        }
    }

    @Override
    /**
     * deletes all rows that matches ALL of the specified entries (AND operator) from the table.
     * @param tableName name of the table to delete the rows from
     * @param columnNameValue the entries to which rows will be compared with
     * @throws ClassNotFoundException If an error occurred in the stored table pages format
     * @throws IOException If an I/O error occurred
     * @throws DBAppException If an an error occurred in the table(No rows are deleted,table not found,types don't match,...)
     * @throws ParseException If an error occurred while parsing the input
     */
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        //validate input, get clusteringKey
        String clusteringKey = Table.validateDeleteFromTable(tableName, columnNameValue);
        //Read the table from disk
        String tablePath = "src/main/resources/data/Tables/" + tableName + ".ser";
        Table targetTable = (Table) diskHandler.deserializeObject(tablePath);
        //check if the clustering key exists in the entries
        boolean clusteringKeyExists = columnNameValue.get(clusteringKey) != null;
        boolean deleted;
        if (clusteringKeyExists)
            deleted = targetTable.deleteFromTableBinarySearch( columnNameValue, clusteringKey);//binary search for the clustering key
        else
            deleted = targetTable.deleteFromTable(columnNameValue, clusteringKey);
        if (!deleted) {
            System.out.println("No Rows matching the entries were found. Row: " + columnNameValue);
            return;
        }
        //update the table's file in disk
        diskHandler.delete(tablePath);
        diskHandler.serializeObject(targetTable, tablePath);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws IOException, DBAppException, ClassNotFoundException, ParseException {
        if (sqlTerms.length - 1 != arrayOperators.length)
            throw new DBAppException("Number of terms and operators does not match.");

        String targetTableName = sqlTerms[0].get_strTableName();
        SQLTerm.validateArrayOperators(arrayOperators);
        String clusteringColumnName = Table.validateExistingTable(targetTableName);
        SQLTerm.validateTerms(sqlTerms);

        // getting the table Object we want to select from
        Table targetTable = (Table) diskHandler.deserializeObject("src/main/resources/data/Tables/" + targetTableName + ".ser");
        Object[] tableInfo = Table.getTableInfo(targetTableName);
        Vector<Page> tablePages = targetTable.getPages();
        Stack<Vector<Hashtable<String, Object>>> termsSets = new Stack<>();

        Vector<Pair> indicesWithTerms = SQLTerm.isIndexPreferable(sqlTerms, arrayOperators, targetTable);
        HashMap<String, Vector<SQLTerm>> hashMapOfTerms = SQLTerm.hashingTerms(sqlTerms);

        if (indicesWithTerms != null) {
            for (Pair pair : indicesWithTerms) {
                if (pair.terms.isEmpty())
                    continue;
                Index index = pair.getIndex();
                Vector<SQLTerm> indexTerms = pair.getTerms();
                if (index != null) {
                    Hashtable<String, Range> termsRanges = new Hashtable<>();
                    for (SQLTerm indexTerm : indexTerms) {
                        Vector<SQLTerm> terms = hashMapOfTerms.get(indexTerm.get_strColumnName());
                        Range range = new Range((Comparable) ((Hashtable<String, Object>) tableInfo[1]).get(indexTerm.get_strColumnName()), (Comparable) ((Hashtable<String, Object>) tableInfo[2]).get(indexTerm.get_strColumnName()));
                        for (SQLTerm term : terms) {
                            range = term.updateColumnRange(range);
                            if (range == null)
                                return new Vector<>().iterator();
                        }
                        termsRanges.put(indexTerm.get_strColumnName(), range);
                    }
                    Vector<Bucket> buckets = new Vector<>();
                    Object grid = diskHandler.deserializeObject(index.getPath());
                    Vector<Vector<Bucket>> cells = index.searchInsideIndex(grid, termsRanges);
                    for (Vector<Bucket> vector : cells)
                        buckets.addAll(vector);
                    Vector<SQLTerm> terms = new Vector<>();
                    for (SQLTerm indexTerm : indexTerms)
                        terms.addAll(hashMapOfTerms.get(indexTerm.get_strColumnName()));
                    Vector<Hashtable<String, Object>> vector = SQLTerm.getValidRowsInBucket(terms, buckets, clusteringColumnName);
                    termsSets.add(vector);
                } else {
                    for (SQLTerm nonIndexedTerm : pair.getTerms()) {
                        Vector<SQLTerm> terms = hashMapOfTerms.get(nonIndexedTerm.get_strColumnName());
                        for (SQLTerm term : terms) {
                            Vector<Hashtable<String, Object>> vector = term.isValidTerm(tablePages, clusteringColumnName);
                            termsSets.add(vector);
                        }
                    }
                }
            }
            while (termsSets.size() > 1) {
                Vector<Hashtable<String, Object>> a = termsSets.pop();
                Vector<Hashtable<String, Object>> b = termsSets.pop();
                Vector<Hashtable<String, Object>> aIntersectB = Table.rowsIntersection(a, b, clusteringColumnName);
                termsSets.push(aIntersectB);
            }
            Vector<Hashtable<String, Object>> queryResult;
            queryResult = termsSets.pop();
            return queryResult.iterator();
        }

        Vector<Vector<Hashtable<String, Object>>> operands = new Vector<>();

        for (int i = 0; i < sqlTerms.length; i++) {
            Vector<Hashtable<String, Object>> vector = sqlTerms[i].isValidTerm(tablePages, clusteringColumnName);
            operands.add(vector);
        }

        return selectPrecedence(operands, arrayOperators, clusteringColumnName).iterator();
    }


    private Vector<Hashtable<String, Object>> selectPrecedence(Vector<Vector<Hashtable<String, Object>>> operands, String[] operators, String clusteringColumnName) {

        Stack<String> operatorsForPostfix = new Stack<>();
        Vector<Object> postfixExp = new Vector<>();

        for (int i = 0; i < operands.size(); i++) {
            if (i == 0)
                postfixExp.add(operands.get(i));
            else {

                if (operatorsForPostfix.isEmpty()) {
                    operatorsForPostfix.push(operators[i - 1]);
                } else if (getOperatorPriority(operatorsForPostfix.peek()) < getOperatorPriority(operators[i - 1])) {
                    operatorsForPostfix.push(operators[i - 1]);
                } else {
                    while (!operatorsForPostfix.isEmpty() &&
                            getOperatorPriority(operatorsForPostfix.peek()) >= getOperatorPriority(operators[i - 1])) {
                        postfixExp.add(operatorsForPostfix.pop());
                    }
                    operatorsForPostfix.push(operators[i - 1]);
                }
                postfixExp.add(operands.get(i));
            }
        }

        while (!operatorsForPostfix.isEmpty())
            postfixExp.add(operatorsForPostfix.pop());

        Stack<Vector<Hashtable<String, Object>>> evalExp = new Stack<>();

        for (Object o : postfixExp) {
            if (o instanceof String) {
                Vector<Hashtable<String, Object>> a = evalExp.pop();
                Vector<Hashtable<String, Object>> b = evalExp.pop();

                String op = (String) o;
                switch (op) {
                    case "AND":
                        Vector<Hashtable<String, Object>> aIntersectB = Table.rowsIntersection(a, b, clusteringColumnName);
                        evalExp.push(aIntersectB);
                        break;
                    case "OR":
                        Vector<Hashtable<String, Object>> aUnionB = Table.rowsUnion(a, b, clusteringColumnName);
                        evalExp.push(aUnionB);
                        break;
                    case "XOR":
                        Vector<Hashtable<String, Object>> aDiffB = Table.rowsDifference(a, b, clusteringColumnName);
                        Vector<Hashtable<String, Object>> bDiffA = Table.rowsDifference(b, a, clusteringColumnName);
                        Vector<Hashtable<String, Object>> aXorB = Table.rowsUnion(aDiffB, bDiffA, clusteringColumnName);
                        evalExp.push(aXorB);
                        break;
                }
            } else {
                evalExp.push((Vector<Hashtable<String, Object>>) o);
            }
        }
        return evalExp.pop();
    }

    private int getOperatorPriority(String operator) {
        switch (operator) {
            case "AND":
                return 3;
            case "XOR":
                return 2;
            case "OR":
                return 1;
            default:
                return -1;
        }
    }

    @Override
    public Iterator parseSQL(StringBuffer strBufSQL) throws DBAppException {
        SQLiteLexer lexer = new SQLiteLexer(CharStreams.fromString(strBufSQL.toString()));
        MiniSQLParser parser = new MiniSQLParser(new CommonTokenStream(lexer));
        ParserErrorHandler errorHandler = new ParserErrorHandler();
        parser.setErrorHandler(errorHandler);
        ParseTree tree = parser.start();
        ParseTreeWalker walker = new ParseTreeWalker();
        ParserListener listener = new ParserListener(this);
        walker.walk(listener, tree);

        return listener.getIterator();
    }

    public static void main(String[] args) throws DBAppException {
        DBApp dbApp = new DBApp();
        Iterator i = dbApp.parseSQL(new StringBuffer("select * from students where gpa >= 3.03"));
        int c = 0;
        while (i.hasNext()){
            System.out.println(i.next());
            c++;
        }
        System.out.println(c);
    }

}
