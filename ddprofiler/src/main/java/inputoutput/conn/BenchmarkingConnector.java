package inputoutput.conn;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import inputoutput.Attribute;
import inputoutput.Record;

public class BenchmarkingConnector extends Connector {

	private int pointer = 0;
	private List<Attribute> attributes;
	private List<Record> records;
	
	public BenchmarkingConnector(BenchmarkingData data) {
		this.attributes = data.attributes;
		this.records = data.records;
	}
	
	public static BenchmarkingConnector makeOne(BenchmarkingData benchData) {
		BenchmarkingConnector c = new BenchmarkingConnector(benchData);
		return c;
	}
	
	@Override
	public String getDBName() {
		return "benchmarking";
	}

	@Override
	public String getPath() {
		return "benchmarking";
	}

	@Override
	public String getSourceName() {
		return "benchmarking";
	}

	@Override
	void initConnector() throws IOException, ClassNotFoundException, SQLException {
		
	}

	@Override
	void destroyConnector() {
		this.close();
	}

	@Override
	public List<Attribute> getAttributes() throws IOException, SQLException {
		return attributes;
	}

	@Override
	public boolean readRows(int num, List<Record> rec_list) throws IOException, SQLException {
		while (num > 0) {
			num--;
			if(pointer == records.size()) {
				return false;
			}
			Record r = records.get(pointer);
			pointer++;
			if(r == null) {
				return false;
			}
			rec_list.add(r);
		}
		return true;
	}

	@Override
	public void close() {
		attributes = null;
		records = null;
	}

}
