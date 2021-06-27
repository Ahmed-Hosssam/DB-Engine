import java.io.*;
import java.util.*;

public class Page implements Serializable {

    private String path;
    private Object minClusteringValue;
    private Object maxClusteringValue;
    private int numOfRecords;
    private final DiskHandler diskHandler = new DiskHandler();

    public Page(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Object getMinClusteringValue() {
        return minClusteringValue;
    }

    public void setMinClusteringValue(Object minClusteringValue) {
        this.minClusteringValue = minClusteringValue;
    }

    public Object getMaxClusteringValue() {
        return maxClusteringValue;
    }

    public void setMaxClusteringValue(Object maxClusteringValue) {
        this.maxClusteringValue = maxClusteringValue;
    }

    public int getNumOfRecords() {
        return numOfRecords;
    }

    public void setNumOfRecords(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    public String insertRecordInPlace(int idxOfRecord, Hashtable<String, Object> colNameValue, String primaryKey, Vector<Hashtable<String, Object>> pageRecords) throws IOException {
        idxOfRecord -= getNumOfRecords();
        pageRecords.add(idxOfRecord, colNameValue);
        setMinClusteringValue(pageRecords.get(0).get(primaryKey));
        setMaxClusteringValue(pageRecords.get(pageRecords.size() - 1).get(primaryKey));
        setNumOfRecords(pageRecords.size());
        diskHandler.delete(getPath());
        diskHandler.serializeObject(pageRecords, getPath());
        return getPath();
    }

    static int searchInsidePage(Vector<Hashtable<String, Object>> page, Object clusteringObject, String clusteringCol) {
        int lo = 0;
        int hi = page.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (DBApp.compare(clusteringObject, page.get(mid).get(clusteringCol)) == 0) {
                return mid;
            } else if (DBApp.compare(clusteringObject, page.get(mid).get(clusteringCol)) < 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return hi + 1 + page.size();
    }


    /**
     * deletes all rows that matches ALL of the specified entries(AND operator) from the page.
     *
     * @param columnNameValue the column key-value pairs to which records will be compared with
     * @param clusteringKey   the clustering key of the page's table
     * @param deletedRows
     * @return 0 if no rows are deleted,-1 if all rows are deleted and 1 if some (but not all) rows are deleted.
     * @throws ClassNotFoundException If an error occurred in the stored table pages format
     * @throws IOException            If an I/O error occurred
     */
    public int deleteFromPage(Hashtable<String, Object> columnNameValue, String clusteringKey, HashSet<Object> deletedRows) throws IOException, ClassNotFoundException {
        //read the page from disk
        Vector<Hashtable<String, Object>> rows = (Vector<Hashtable<String, Object>>) diskHandler.deserializeObject(getPath());
        int state = 0;//return state
        //iterate through the page to compare
        if (columnNameValue.get(clusteringKey) != null) {
            int idxInsidePage = Page.searchInsidePage(rows, columnNameValue.get(clusteringKey), clusteringKey);
            if (idxInsidePage < rows.size()) {
                boolean delete = true;
                Hashtable<String, Object> curRow = rows.get(idxInsidePage);
                for (String key : columnNameValue.keySet())
                    delete &= curRow.containsKey(key) && DBApp.compare(curRow.get(key), columnNameValue.get(key)) == 0;
                if (delete) {
                    deletedRows.add(rows.get(idxInsidePage).get(clusteringKey));
                    rows.remove(idxInsidePage);
                    state = 1;
                }

            }
        } else {
            Iterator<Hashtable<String, Object>> rowsIterator = rows.iterator();
            while (rowsIterator.hasNext()) {
                boolean delete = true;
                Hashtable<String, Object> curRow = rowsIterator.next();
                //compare all entries of the row to be deleted with the current row's entries
                for (String key : columnNameValue.keySet())
                    delete &= curRow.containsKey(key) && DBApp.compare(curRow.get(key), columnNameValue.get(key)) == 0;//checks if the curRow contains the key and the values match

                if (delete) {
                    deletedRows.add(curRow.get(clusteringKey));
                    rowsIterator.remove();
                    state = 1;
                }
            }
        }
        diskHandler.delete(getPath());
        if (rows.isEmpty())
            return -1;
        //update the max, min clustering keys and the number of records
        setNumOfRecords(rows.size());
        setMaxClusteringValue(rows.get(rows.size() - 1).get(clusteringKey));
        setMinClusteringValue(rows.get(0).get(clusteringKey));
        diskHandler.serializeObject(rows, getPath());
        return state;
    }

}
