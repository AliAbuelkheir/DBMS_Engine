import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DBApp
{
	public static int dataPageSize = 2;

	public static void createTable(String tableName, String[] columnsNames)
	{
		String trace = "Table created name:" + tableName + ", columnNames:" + Arrays.toString(columnsNames);
		Table t = new Table(tableName, columnsNames);
		t.addTrace(trace);
	}

	public static void insert(String tableName, String[] record)
	{
		long startTime = System.currentTimeMillis(); // Start timing
		Table t = FileManager.loadTable(tableName);
		Page page = t.getLastPage();
		// First insert
		if(page==null){
			page = t.newPage();;
		}
		// free space in page
		if(page.getRecordCount() >= dataPageSize){
			page = t.newPage();
		}
		page.insertRecord(record);

		long endTime = System.currentTimeMillis(); // End timing
		long duration = endTime - startTime;
		String trace = "Inserted:" + Arrays.toString(record)
				+ ", at page number:" + page.getPageNo()
				+ ", execution time (mil):" + duration;
		t.addTrace(trace);

		FileManager.storeTablePage(tableName, page.getPageNo(), page);
	}


	public static ArrayList<String []> select(String tableName)
	{
		long start = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = new ArrayList<>();
		for (int i=0; i<t.getPageCount(); i++) {
			Page p = FileManager.loadTablePage(tableName, i);
			for( String[] row : p.getData()){
				res.add(row);
			}
		}
		int totalRecords = t.countRecords(dataPageSize);
		long endTime = System.currentTimeMillis();
		long duration = endTime - start;
		t.addTrace("Select all pages:" + t.getPageCount() + ", records:" + res.size() + ",execution time (mil): " + duration);
		return res;
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		long start = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = new ArrayList<>();
		Page p = FileManager.loadTablePage(tableName, pageNumber);
		if (p == null || recordNumber >= p.getRecordCount()) {
			long end = System.currentTimeMillis();
			long duration = end - start;
			t.addTrace("Select pointer page:" + pageNumber + ", record:" + recordNumber + ", total output count: " + res.size() + ", execution time (mil):" + duration);
			return res;
		}
		res.add(p.getData().get(recordNumber));

		long end = System.currentTimeMillis();
		long duration = end - start;

		t.addTrace("Select pointer page:" + pageNumber + ", record:" + recordNumber + ", total output count: " + res.size() + ", execution time (mil):" + duration);
		return res;
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		long start = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = new ArrayList<>();
		int[] colIndex = t.getColumnIndex(cols);


		StringBuilder conditionBuilder = new StringBuilder("Select condition: ");
		conditionBuilder.append(Arrays.toString(cols))
				.append("->")
				.append(Arrays.toString(vals));

		ArrayList<String> pagesCounts = new ArrayList<String>();

		for(int count=0; count<t.getPageCount(); count++){
			Page p = FileManager.loadTablePage(tableName, count);
			int recordsFoundPerPage = 0;

			for(String[] row : p.getData()){
				boolean matches = true;
				for(int i=0; i < colIndex.length; i++){
					if(!row[colIndex[i]].equals(vals[i])){
						matches = false;
						break;
					}
				}
				if(matches){
					res.add(row);
					recordsFoundPerPage++;
				}
			}

			if(recordsFoundPerPage > 0) {
				pagesCounts.add("[" + count + ", " + recordsFoundPerPage + "]");
			}
		}

		conditionBuilder.append(", Records per page:")
				.append(pagesCounts.toString());

		long end = System.currentTimeMillis();
		long duration = end - start;

		conditionBuilder.append(", records:")
				.append(res.size())
				.append(", execution time (mil):")
				.append(duration);

		t.addTrace(conditionBuilder.toString());
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName)
	{
		Table table = FileManager.loadTable(tableName);
		return table.getFullTrace(dataPageSize);
	}

	public static String getLastTrace(String tableName)
	{
		Table table = FileManager.loadTable(tableName);
		return table.getLastTrace();
	}


	public static void main(String []args) throws IOException
	{

	}
}
