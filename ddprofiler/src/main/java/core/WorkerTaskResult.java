package core;


public class WorkerTaskResult {

	final private int id;
	final private String sourceName;
	final private String columnName;
	final private String dataType;
	final private int totalValues;
	final private int uniqueValues;
	final private String entities;
	final private float minValue;
	final private float maxValue;
	final private float avgValue;
	
	public WorkerTaskResult(
			int id,
			String sourceName,
			String columnName,
			String dataType,
			int totalValues,
			int uniqueValues,
			String entities) {
		this.id = id;
		this.sourceName = sourceName;
		this.columnName = columnName;
		this.dataType = dataType;
		this.totalValues = totalValues;
		this.uniqueValues = uniqueValues;
		this.entities = entities;
		this.minValue = 0; // non existent
		this.maxValue = 0; // non existent
		this.avgValue = 0; // non existent
	}
	
	public WorkerTaskResult(
			int id,
			String sourceName,
			String columnName,
			String dataType,
			int totalValues,
			int uniqueValues,
			float minValue,
			float maxValue,
			float avgValue) {
		this.id = id;
		this.sourceName = sourceName;
		this.columnName = columnName;
		this.dataType = dataType;
		this.totalValues = totalValues;
		this.uniqueValues = uniqueValues;
		this.entities = ""; // non existent
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.avgValue = avgValue;
	}
	
	public int getId() {
		return id;
	}

	public String getSourceName() {
		return sourceName;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getDataType() {
		return dataType;
	}

	public int getTotalValues() {
		return totalValues;
	}

	public int getUniqueValues() {
		return uniqueValues;
	}

	public String getEntities() {
		return entities;
	}

	public float getMinValue() {
		return minValue;
	}

	public float getMaxValue() {
		return maxValue;
	}

	public float getAvgValue() {
		return avgValue;
	}

}
