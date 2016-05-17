/**
 * @author Raul - raulcf@csail.mit.edu
 *
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
	private double avg;
	
	public Range(Type t, int totalRecords, int max, int min, double avg) {
		this.type = t;
		this.totalRecords = totalRecords;
		this.max = max;
		this.min = min;
		this.avg = avg;
	}
	
	public Range(Type t, int totalRecords, float maxF, float minF, double avg) {
		this.type = t;
		this.totalRecords = totalRecords;
		this.maxF = maxF;
		this.minF = minF;
		this.avg = avg;
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

	public double getAvg() {
		return avg;
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
