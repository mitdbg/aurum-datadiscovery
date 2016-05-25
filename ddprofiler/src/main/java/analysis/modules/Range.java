/**
 * @author Raul - raulcf@csail.mit.edu
 * @author Sibo Wang (edit)
 */
package analysis.modules;

import analysis.modules.DataType.Type;

public class Range {

	private Type type;

	private int totalRecords;
	private int max;
	private float maxF;
	private int min;
	private float minF;
	private float avg;
	private float stdDeviation;//stores the stdDeviation for the profiler
	
	public Range(Type t, int totalRecords, int max, int min, float avg, float stdDeviation) {
		this.type = t;
		this.totalRecords = totalRecords;
		this.max = max;
		this.min = min;
		this.avg = avg;
		this.stdDeviation = stdDeviation;
	}
	
	public Range(Type t, int totalRecords, float maxF, float minF, float avg, float stdDeviation) {
		this.type = t;
		this.totalRecords = totalRecords;
		this.maxF = maxF;
		this.minF = minF;
		this.avg = avg;
		this.stdDeviation = stdDeviation;
	}
	
	public Type getType() {
		return type;
	}

	public int getTotalRecords() {
		return totalRecords;
	}

	public int getMax() {
		return max;
	}

	public float getMaxF() {
		return maxF;
	}

	public int getMin() {
		return min;
	}

	public float getMinF() {
		return minF;
	}

	public float getAvg() {
		return avg;
	}

	
	public float getStdDeviation() {
		return stdDeviation;
	}

	public void setStdDeviation(float stdDeviation) {
		this.stdDeviation = stdDeviation;
	}

	
	
	@Override
	public String toString() {
		if(type.equals(Type.INT)) {
			return "Max: "+max+" Min:"+min+" Avg: "+avg;
		}
		else if(type.equals(Type.FLOAT)) {
			return "Max: "+maxF+" Min:"+minF+" Avg: "+avg;
		}
		else {
			return "Unknown type";
		}
	}


}
