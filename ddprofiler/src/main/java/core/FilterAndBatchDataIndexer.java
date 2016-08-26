package core;

import java.util.Map;
import java.util.Map.Entry;

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import preanalysis.Values;
import store.Store;

public class FilterAndBatchDataIndexer implements DataIndexer {

	private Store store;
	
	public FilterAndBatchDataIndexer(Store s) {
		this.store = s;
	}
	
	/**
	 * TODO: For now simply make sure upserts work well. In the (near) future, prepare
	 * a filtering mechanism here, fixed to the context of a source that removes data per
	 * column that has already been indexed. Chances are we can reduce the amount of data
	 * to index a lot!
	 * 
	 * Final version will do, per field:
	 * - filter out already seen data (data that has been seen with high probability)
	 * - batch data locally until there's an important amount (configurable)
	 * - then call the store (which may or may not batch the request)
	 * 
	 * For now:
	 * - create update request on the same document to include more terms (all of them)
	 * - each call to indexData involves deleting, creating, reindexing doc, so we want
	 *   to do it as few times as possible
	 */
	@Override
	public boolean indexData(Map<Attribute, Values> data, String sourceName) {
		for(Entry<Attribute, Values> entry : data.entrySet()) {
			Attribute a = entry.getKey();
			AttributeType at = a.getColumnType();
			
			if(at.equals(AttributeType.STRING)) {
				String columnName = a.getColumnName();
				int id = Utils.computeAttrId(sourceName, columnName);
				store.indexData(id, sourceName, columnName, entry.getValue().getStrings());
			}
		}
		return true;
	}

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		return false;
	}

}
