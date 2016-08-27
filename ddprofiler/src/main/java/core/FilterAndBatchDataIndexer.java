package core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import inputoutput.Attribute;
import inputoutput.Attribute.AttributeType;
import preanalysis.Values;
import store.Store;

public class FilterAndBatchDataIndexer implements DataIndexer {

	private Store store;
	private Map<String, Integer> attributeIds;
	private Map<String, List<String>> attributeValues;
	private Map<String, Integer> attributeValueSize;
	private int indexTriggerThreshold = 1 * 1024 * 1024 * 35; // 35 MB 
	
	public FilterAndBatchDataIndexer(Store s) {
		this.store = s;
		this.attributeIds = new HashMap<>();
		this.attributeValues = new HashMap<>();
		this.attributeValueSize = new HashMap<>();
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
//				// Id for the attribute - computed only once
//				int id = 0;
//				if( ! attributeIds.containsKey(columnName)) {
//					id = Utils.computeAttrId(sourceName, columnName);
//					attributeIds.put(columnName, id);
//				}
//				else {
//					id = attributeIds.get(columnName);
//				}
				
				// FIXME: introduce new call
				int id = Utils.computeAttrId(sourceName, columnName);
				store.indexData(id, sourceName, columnName, entry.getValue().getStrings());
			}
		}
		return true;
	}
	
	private void storeNewValuesAndMaybeTriggerIndex(int id, String sourceName, String columnName, List<String> newValues) {
		if(! attributeValues.containsKey(columnName)) {
			attributeValues.put(columnName, new ArrayList<>());
			attributeValueSize.put(columnName, 0);
		}
		int size = computeAproxSizeOf(newValues);
		int currentSize = attributeValueSize.get(columnName);
		int newSize = currentSize + size;
		attributeValueSize.put(columnName, newSize);
		updateValues(columnName, newValues);
		if(newSize > indexTriggerThreshold) {
			// Index the batch of values
			List<String> values = attributeValues.get(columnName);
			store.indexData(id, sourceName, columnName, values);
			// Clean up
			attributeValues.put(columnName, new ArrayList<>());
			attributeValueSize.put(columnName, 0);
		}
	}
	
	private void updateValues(String columnName, List<String> values) {
		List<String> currentValues = attributeValues.get(columnName);
		currentValues.addAll(values);
		attributeValues.put(columnName, currentValues);
	}
	
	private int computeAproxSizeOf(List<String> values) {
		int size = 0;
		for(String s : values) {
			size += s.length();
		}
		return size;
	}

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		return false;
	}

}
