import java.io.*;
import java.util.*;

public class SQLTerm implements Comparable<Object> {


    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    private final static DiskHandler diskHandler = new DiskHandler();

    public String get_strTableName() {
        return _strTableName;
    }

    public String get_strColumnName() {
        return _strColumnName;
    }

    public String get_strOperator() {
        return _strOperator;
    }

    public Object get_objValue() {
        return _objValue;
    }

    @Override
    public int compareTo(Object o) {
        if (this.get_objValue() instanceof java.util.Date)
            return ((Date) this.get_objValue()).compareTo((Date) o);
        else if (this.get_objValue() instanceof java.lang.Integer)
            return ((Integer) this.get_objValue()).compareTo((Integer) o);
        else if (this.get_objValue() instanceof java.lang.Double)
            return ((Double) this.get_objValue()).compareTo((Double) o);
        else if (this.get_objValue() instanceof java.lang.String)
            return ((String) this.get_objValue()).toLowerCase().compareTo(((String) o).toLowerCase());
        return 0;

    }

    public Range updateColumnRange(Range range) throws DBAppException {
        Range newRange = null;
        switch (get_strOperator()) {
            case "=":
                newRange = ((Comparable) get_objValue()).compareTo(range.getMinVal()) >= 0 && ((Comparable) get_objValue()).compareTo(range.getMaxVal()) <= 0
                        ? new Range((Comparable) get_objValue(), (Comparable) get_objValue()) : null;
                break;
            case ">":
            case ">=":
                newRange = new Range(((Comparable) get_objValue()).compareTo(range.getMinVal()) > 0
                        ? (Comparable) get_objValue() : range.getMinVal(), range.getMaxVal());
                break;
            case "<":
            case "<=":
                newRange = new Range(range.getMinVal(), ((Comparable) get_objValue()).compareTo(range.getMaxVal()) < 0
                        ? (Comparable) get_objValue() : range.getMaxVal());
        }
        if (newRange != null && newRange.getMaxVal().compareTo(newRange.getMinVal()) < 0)
            newRange = null;
        return newRange;
    }

    public Vector<Hashtable<String, Object>> isValidTerm(Vector<Page> tablePages, String clusteringColumnName) throws IOException, ClassNotFoundException {
        if (get_strColumnName().equals(clusteringColumnName))
            return searchOnClustering(tablePages);
        return searchLinearly(tablePages);

    }

    private Vector<Hashtable<String, Object>> searchLinearly(Vector<Page> tablePages) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (Page page : tablePages) {
            Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(page.getPath());
            for (Hashtable<String, Object> row : rows)
                if (booleanValueOfTerm(row))
                    result.add(row);
        }
        return result;
    }

    private Vector<Hashtable<String, Object>> searchOnClustering(Vector<Page> tablePages) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        if (tablePages.size() == 0)
            return result;
        int lo = 0;
        int hi = tablePages.size() - 1;
        Page targetPage = tablePages.get(0);
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            Page curPage = tablePages.get(mid);
            if (compareTo(curPage.getMinClusteringValue()) >= 0 && compareTo(curPage.getMaxClusteringValue()) <= 0) {
                targetPage = curPage;
                break;
            }
            if (compareTo(curPage.getMinClusteringValue()) < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }

        Vector<Hashtable<String, Object>> targetPageRows = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(targetPage.getPath());
        if (get_strOperator().equals("=")) {
            lo = 0;
            hi = targetPageRows.size() - 1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                Hashtable<String, Object> curRow = targetPageRows.get(mid);
                if (booleanValueOfTerm(curRow)) {
                    result.add(curRow);
                    break;
                }
                if (compareTo(curRow.get(get_strColumnName())) < 0)
                    hi = mid - 1;
                else
                    lo = mid + 1;
            }

            return result;
        }

        Vector<Page> pagesBeforeTarget = new Vector<>();
        Vector<Page> pagesAfterTarget = new Vector<>();
        for (Page page : tablePages) {
            if (page.equals(targetPage))
                break;
            pagesBeforeTarget.add(page);
        }
        for (int i = tablePages.indexOf(targetPage) + 1; i < tablePages.size(); i++)
            pagesAfterTarget.add(tablePages.get(i));

        for (Hashtable<String, Object> row : targetPageRows)
            if (booleanValueOfTerm(row))
                result.add(row);

        if (get_strOperator().equals("<") || get_strOperator().equals("<=")) {
            addRowsOfPage(result, pagesBeforeTarget);
        } else if (get_strOperator().equals(">") || get_strOperator().equals(">=")) {
            addRowsOfPage(result, pagesAfterTarget);
        } else {
            addRowsOfPage(result, pagesBeforeTarget);
            addRowsOfPage(result, pagesAfterTarget);
        }
        return result;
    }

    private void addRowsOfPage(Vector<Hashtable<String, Object>> result, Vector<Page> pages) throws IOException, ClassNotFoundException {
        for (Page page : pages) {
            FileInputStream fileInputStream = new FileInputStream(page.getPath());
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            result.addAll((Vector<Hashtable<String, Object>>) objectInputStream.readObject());
            objectInputStream.close();
            fileInputStream.close();
        }
    }

    public boolean booleanValueOfTerm(Hashtable<String, Object> row) {
        switch (get_strOperator()) {
            case "=":
                return (compareTo(row.get(get_strColumnName())) == 0);
            case "!=":
                return (compareTo(row.get(get_strColumnName())) != 0);
            case ">":
                return (compareTo(row.get(get_strColumnName())) < 0);
            case ">=":
                return (compareTo(row.get(get_strColumnName())) <= 0);
            case "<":
                return (compareTo(row.get(get_strColumnName())) > 0);
            case "<=":
                return (compareTo(row.get(get_strColumnName())) >= 0);
            default:
                return false;
        }
    }

    static Vector<Hashtable<String, Object>> getValidRowsInBucket(Vector<SQLTerm> terms, Vector<Bucket> buckets, String clusteringColumnName) throws IOException, ClassNotFoundException {
        Hashtable<String, Vector<Object>> rows = new Hashtable<>();
        for (Bucket bucket : buckets) {
            Hashtable<Hashtable<String, Object>, Vector<RowReference>> bucketObject = (Hashtable<Hashtable<String, Object>, Vector<RowReference>>) diskHandler.deserializeObject(bucket.getPath());
            for (Hashtable<String, Object> key : bucketObject.keySet()) {
                boolean valid = true;
                for (SQLTerm term : terms)
                    if (!term.booleanValueOfTerm(key)) {
                        valid = false;
                        break;
                    }
                if (valid) {
                    Vector<RowReference> rowReferences = bucketObject.get(key);
                    for (RowReference rowReference : rowReferences) {
                        Vector<Object> clusteringValues = new Vector<>();
                        if (rows.containsKey(rowReference.getPagePath()))
                            clusteringValues = rows.get(rowReference.getPagePath());
                        clusteringValues.add(rowReference.getClusteringValue());
                        rows.put(rowReference.getPagePath(), clusteringValues);
                    }
                }
            }

        }
        return getCorrespondingRows(rows, clusteringColumnName);
    }

    private static Vector<Hashtable<String, Object>> getCorrespondingRows(Hashtable<String, Vector<Object>> gridRecords, String clusteringColumnName) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (String pagePath : gridRecords.keySet()) {
            Vector<Hashtable<String, Object>> currentPage = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(pagePath);
            for (Object value : gridRecords.get(pagePath)) {
                int index = Page.searchInsidePage(currentPage, value, clusteringColumnName);
                result.add(currentPage.get(index));
            }
        }
        return result;
    }

    static void validateArrayOperators(String[] arrayOperators) throws DBAppException {
        for (String operator : arrayOperators) {
            if (!(operator.equals("OR") || operator.equals("AND") || operator.equals("XOR")))
                throw new DBAppException("The operator is not correct");
        }
    }

    static void validateTerms(SQLTerm[] sqlTerms) throws IOException, DBAppException, ClassNotFoundException {
        String targetTableName = sqlTerms[0].get_strTableName();
        for (SQLTerm term : sqlTerms) {
            if (!term.get_strTableName().equals(targetTableName)) {
                throw new DBAppException("The table name in all terms must be the same.");
            }
        }

        FileReader metadata = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(metadata);
        String curLine;
        Hashtable<String, String> colDataTypes = new Hashtable<>();
        while ((curLine = br.readLine()) != null) {
            String[] res = curLine.split(",");
            if (res[0].equals(sqlTerms[0].get_strTableName())) {
                colDataTypes.put(res[1], res[2]);
            }
        }
        for (SQLTerm term : sqlTerms) {
            String columnName = term.get_strColumnName();
            Object columnValue = term.get_objValue();
            if (!colDataTypes.containsKey(columnName)) {
                throw new DBAppException("Column does not exist");
            }
            Class colClass = Class.forName(colDataTypes.get(columnName));
            if (!colClass.isInstance(columnValue)) {
                throw new DBAppException("Incompatible data types");
            }
        }
    }

    static HashMap<String, Vector<SQLTerm>> hashingTerms(SQLTerm[] sqlTerms) {
        HashMap<String, Vector<SQLTerm>> hashMap = new HashMap<>();
        for (SQLTerm term : sqlTerms)
            hashMap.put(term.get_strColumnName(), new Vector<>());
        for (SQLTerm term : sqlTerms)
            hashMap.get(term.get_strColumnName()).add(term);
        return hashMap;
    }

    static Vector<DBApp.Pair> isIndexPreferable(SQLTerm[] sqlTerms, String[] arrayOperators, Table targetTable) {
        for (String operator : arrayOperators)
            if (!operator.equals("AND"))
                return null;
        for (SQLTerm term : sqlTerms)
            if (term._strOperator.equals("!="))
                return null;

        Vector<String> termsColumnNames = new Vector<>();
        boolean[] termsVisited = new boolean[sqlTerms.length];
        for (SQLTerm term : sqlTerms)
            termsColumnNames.add(term.get_strColumnName());
        Vector<Index> tableIndices = targetTable.getIndices();
        Vector<DBApp.Pair> termsOfIndices = new Vector<>();
        Collections.sort(tableIndices, (a, b) -> b.getColumnsCount() - a.getColumnsCount());
        HashSet<String> indexedColumns = new HashSet<>();
        for (Index index : tableIndices) {
            Vector<SQLTerm> validTerms = new Vector<>();
            String[] columnNames = index.getColumnNames();
            for (int i = 0; i < columnNames.length; i++) {
                String dimensionName = columnNames[i];
                if (termsColumnNames.contains(dimensionName)
                        && !termsVisited[termsColumnNames.indexOf(dimensionName)]) {
                    validTerms.add(sqlTerms[termsColumnNames.indexOf(dimensionName)]);
                    termsVisited[termsColumnNames.indexOf(dimensionName)] = true;
                    indexedColumns.add(dimensionName);
                    if (i == index.getColumnNames().length - 1) {
                        DBApp.Pair p = new DBApp.Pair(index, validTerms);
                        termsOfIndices.add(p);
                    }
                    continue;
                }
                DBApp.Pair p = new DBApp.Pair(index, validTerms);
                termsOfIndices.add(p);
                break;
            }
        }
        DBApp.Pair nonIndexedTerms = new DBApp.Pair(null, new Vector<>());
        for (int i = 0; i < termsVisited.length; i++)
            if (!termsVisited[i] && !indexedColumns.contains(sqlTerms[i].get_strColumnName()))
                nonIndexedTerms.add(sqlTerms[i]);

        termsOfIndices.add(nonIndexedTerms);
        if (termsOfIndices.size() == 1 && termsOfIndices.get(0).getIndex() == null)
            return null;
        return termsOfIndices;
    }


}
