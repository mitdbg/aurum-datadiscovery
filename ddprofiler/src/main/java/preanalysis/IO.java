/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;
import java.util.Map;

import inputoutput.Attribute;

public interface IO {

	public Map<Attribute, List<String>> readRows(int num);
	
}
