/**
 * 
 */
/**
 * @author Sibo Wang
 *
 */
package inputoutput;

import java.util.List;
import java.util.Vector;

public class Record {
	private List<String> tuples;
	public Record(){
		tuples = new Vector<String>();
	}
	public void setTuples(List<String> tuples){
		this.tuples = tuples;
	}
	
	public List<String> getTuples(){
		return this.tuples;
	}
	
	public String toString(){
		String res="Record(";
		for(int i=0; i<tuples.size(); i++){
			res+= tuples.get(i)+",";
		}
		res+=")";
		return res;
	}

	
}
