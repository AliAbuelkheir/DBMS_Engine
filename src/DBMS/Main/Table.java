import java.io.Serializable;
import java.util.ArrayList;

public class Table implements Serializable
{
    private String name;
    private ArrayList<String> columns;
    private ArrayList<String> traceOperations;
    private int pageCount;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastTrace() {
        if (traceOperations.isEmpty()) {
            return "";
        }
        return traceOperations.getLast(); // safe now
    }

    public String getFullTrace(int recordsperPage) {
        StringBuilder sb = new StringBuilder();
        for (String trace : traceOperations) {
            sb.append(trace).append("\n");
        }

        // Append page and record counts

        int recordCount = pageCount * recordsperPage;
        recordCount += FileManager.loadTablePage(this.name, pageCount).getRecordCount();


        sb.append("Pages Count: " + ( pageCount+1))
                .append(", Records Count: " + recordCount);

        return sb.toString();
    }


    public void addTrace(String trace) {
        traceOperations.add(trace);
        FileManager.storeTable(this.name, this);
    }

    public Page getLastPage() {
        if (pageCount == -1) {
            return null;
        }
        Page page = FileManager.loadTablePage(this.name, pageCount);
        return page;
    }

    public Page newPage() {
        if (pageCount == -1) {
            pageCount = 0;
            Page page = new Page(pageCount);
            FileManager.storeTablePage(this.name, pageCount, page);
            return page;
        }
        pageCount++;
        Page page = new Page(pageCount);
        FileManager.storeTablePage(this.name, pageCount, page);
        return page;
    }

    public Table(String name, String[] columns) {
        this.name = name;
        ArrayList<String> columnNames = new ArrayList<>();
        for (String column : columns) {
            columnNames.add(column);
        }
        this.columns = columnNames;
        this.traceOperations = new ArrayList<>();
        this.pageCount = -1;
    }

    public int[] getColumnIndex(String[] cols) {
        int[] colIndex = new int[cols.length];
        int counter = 0;
        for (String col : cols) {
            int index = this.columns.indexOf(col);
            if (index != -1) {
                colIndex[counter] = index;
                counter++;
            }
        }
        return colIndex;
    }

    public int countRecords(int recordsPerPage) {
        if(pageCount == -1){
            return 0;
        }
        int recordCount = pageCount * recordsPerPage;
        recordCount += FileManager.loadTablePage(this.name, pageCount).getRecordCount();
        return recordCount;
    }

    public int getPageCount(){
        if(pageCount == -1){
            return 0;
        }
        return pageCount+1;
    }
}
