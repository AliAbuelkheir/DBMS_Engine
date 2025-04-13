import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{
	private int pageNo;
	private ArrayList<String[]> data;
	private int recordCount;

	public int getPageNo() {
		return pageNo;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void insertRecord(String[] record){
		data.add(record);
		recordCount++;
	}

	public Page(int pageNo) {
		this.pageNo = pageNo;
		this.recordCount = 0;
		data = new ArrayList<>();
	}

	public ArrayList<String[]> getData() {
		return data;
	}
}
