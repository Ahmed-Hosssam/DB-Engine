import org.antlr.v4.runtime.tree.ParseTree;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ParserListener extends MiniSQLParserBaseListener {
    private DBApp dbApp;
    private Iterator iterator;
    private ArrayList<String> terminals;

    public ParserListener(DBApp dbApp) {
        this.dbApp = dbApp;
        terminals = new ArrayList<>();
    }

    public Iterator getIterator() {
        return iterator;
    }

    @Override
    public void enterSelect_stmt(MiniSQLParser.Select_stmtContext ctx) {
        try {
            String tableName = ctx.select_core().children.get(3).getText();
            Object[] info = Table.getTableInfo(tableName);
            Hashtable<String, String> colDataTypes = (Hashtable<String, String>) info[0];
            Hashtable<String, Object> colMinValues = (Hashtable<String, Object>) info[1];
            String clusteringType = (String) info[3];
            String clusteringCol = (String) info[4];
            if (ctx.select_core().getChildCount() > 4)
                traverse(ctx.select_core().children.get(5));
            else {
                terminals.add(clusteringCol);
                terminals.add(">=");
                if (clusteringType.equals("java.util.Date")) {
                    DateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd");
                    terminals.add(dateFormat.format(colMinValues.get(clusteringCol)));
                } else
                    terminals.add(colMinValues.get(clusteringCol).toString());
            }
            SQLTerm[] sqlTerms = new SQLTerm[(terminals.size() + 1) / 4];
            String[] operators = new String[sqlTerms.length - 1];
            for (int i = 0; i < terminals.size(); i += 4) {
                SQLTerm term = new SQLTerm();
                term._strColumnName = terminals.get(i);
                term._strOperator = terminals.get(i + 1).equals("<>") ? "!=" : terminals.get(i + 1);
                try {
                    term._objValue = castString(colDataTypes.get(term._strColumnName), terminals.get(i + 2));
                } catch (NullPointerException e) {
                    throw new DBAppException("Wrong column name!");
                }
                term._strTableName = tableName;
                sqlTerms[i / 4] = term;
                if (i / 4 < operators.length)
                    operators[i / 4] = terminals.get(i + 3).toUpperCase();
            }
            iterator = dbApp.selectFromTable(sqlTerms, operators);
        } catch (DBAppException | IOException | ParseException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void enterCreate_table_stmt(MiniSQLParser.Create_table_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText();
            Hashtable<String, String> colNameType = new Hashtable<>();
            Hashtable<String, String> colMin = new Hashtable<>();
            Hashtable<String, String> colMax = new Hashtable<>();
            for (int i = 0; i < ctx.column_def().size(); i++) {
                String colName = ctx.column_def(i).column_name().getText();
                String colType = "";
                switch (ctx.column_def(i).col_type().getText().toLowerCase()) {
                    case "int":
                        colType = "java.lang.Integer";
                        colMin.put(colName, "" + 0);
                        colMax.put(colName, "" + 100000);
                        break;
                    case "varchar":
                        colType = "java.lang.String";
                        colMin.put(colName, "A");
                        colMax.put(colName, "ZZZZZZZZZZ");
                        break;
                    case "date":
                        colType = "java.util.Date";
                        colMin.put(colName, "1700-01-01");
                        colMax.put(colName, "4000-12-31");
                        break;
                    case "decimal":
                        colType = "java.lang.Double";
                        colMin.put(colName, "" + 0.0);
                        colMax.put(colName, "" + 100000.0);
                        break;
                }
                colNameType.put(colName, colType);
            }
            String clusteringKey = "";
            for (int i = 5; i < ctx.children.size(); i += 2) {
                if (ctx.children.get(i).getText().equals("primarykey")) {
                    clusteringKey = ctx.column_def((i - 1) / 2 - 2).column_name().getText();
                }
            }
            dbApp.createTable(tableName, clusteringKey, colNameType, colMin, colMax);
        } catch (DBAppException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void enterCreate_index_stmt(MiniSQLParser.Create_index_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        String[] indexColumns = new String[ctx.column_name().size()];

        for (int i = 0; i < indexColumns.length; i++) {
            indexColumns[i] = ctx.column_name().get(i).getText();
        }

        try {
            dbApp.createIndex(tableName, indexColumns);
        } catch (DBAppException | IOException | ParseException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void enterInsert_stmt(MiniSQLParser.Insert_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText();
            Hashtable<String, Object> colNameValue = new Hashtable<>();
            Hashtable<String, String> colNameType = (Hashtable<String, String>) (Table.getTableInfo(tableName))[0];
            if (ctx.column_name().size() != ctx.literals().size()) {
                throw new DBAppException("Invalid insert statement");
            }
            for (int i = 0; i < ctx.column_name().size(); i++) {
                String colName = ctx.column_name(i).getText();
                Object val;
                try {
                    val = castString(colNameType.get(colName), ctx.literals(i).getText());
                } catch (NullPointerException e) {
                    throw new DBAppException("Wrong column name!");
                }
                colNameValue.put(colName, val);
            }
            dbApp.insertIntoTable(tableName, colNameValue);
        } catch (DBAppException | ParseException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void enterUpdate_stmt(MiniSQLParser.Update_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText();
            Hashtable<String, Object> colNameValue = new Hashtable<>();
            Object[] info = Table.getTableInfo(tableName);
            Hashtable<String, String> colNameType = (Hashtable<String, String>) info[0];
            String clusteringCol = (String) info[4];
            if (!ctx.update_cond().column_name().getText().equals(clusteringCol)) {
                throw new DBAppException("Update condition must be on clustering key");
            }
            for (int i = 0; i < ctx.column_name().size(); i++) {
                String colName = ctx.column_name(i).getText();
                Object val;
                try {
                    val = castString(colNameType.get(colName), ctx.literals(i).getText());
                } catch (NullPointerException e) {
                    throw new DBAppException("Wrong column name!");
                }
                colNameValue.put(colName, val);
            }
            StringTokenizer st = new StringTokenizer(ctx.update_cond().literals().getText(), "'\"");
            String clusteringValue = st.nextToken();
            dbApp.updateTable(tableName, clusteringValue, colNameValue);
        } catch (IOException | ParseException | DBAppException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void enterDelete_stmt(MiniSQLParser.Delete_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText();
            traverse(ctx.limited_expr());
            Hashtable<String, Object> colNameVal = new Hashtable<>();
            Hashtable<String, String> colNameType = (Hashtable<String, String>) (Table.getTableInfo(tableName))[0];
            for (int i = 0; i < terminals.size(); i += 4) {
                try {
                    colNameVal.put(terminals.get(i), castString(colNameType.get(terminals.get(i)), terminals.get(i + 2)));
                } catch (NullPointerException e) {
                    throw new DBAppException("Wrong column name!");
                }
            }
            dbApp.deleteFromTable(tableName, colNameVal);
        } catch (DBAppException | ClassNotFoundException | IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void traverse(ParseTree tree) {
        if (tree.getChildCount() == 0) {
            terminals.add(tree.getText());
            return;
        }
        for (int i = 0; i < tree.getChildCount(); i++)
            traverse(tree.getChild(i));

    }

    private Object castString(String type, String value) throws DBAppException {
        Object returnObject = null;
        switch (type) {
            case "java.lang.Integer":
                try {
                    returnObject = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            case "java.lang.Double":
                try {
                    returnObject = Double.parseDouble(value);
                } catch (NumberFormatException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            case "java.util.Date":
                try {
                    StringTokenizer st = new StringTokenizer(value, "'\"");
                    returnObject = new SimpleDateFormat("yyyy-MM-dd").parse(st.nextToken());
                } catch (ParseException e) {
                    throw new DBAppException("Incompatible clustering key data type");
                }
                break;
            case "java.lang.String":
                StringTokenizer st = new StringTokenizer(value, "'\"");
                returnObject = st.nextToken();
                break;
        }
        return returnObject;
    }
}
