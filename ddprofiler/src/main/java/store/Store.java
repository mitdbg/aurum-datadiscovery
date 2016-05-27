package store;

import java.util.List;

import core.WorkerTaskResult;

public interface Store {

	public void initStore();
	public boolean indexData(int id, List<String> values);
	public boolean storeDocument(WorkerTaskResult wtr);
	public void tearDownStore();
	
}
