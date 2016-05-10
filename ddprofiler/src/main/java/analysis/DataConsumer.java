package analysis;

import java.util.List;

import inputoutput.Record;

public interface DataConsumer {

	public boolean feedData(List<Record> records);
}
