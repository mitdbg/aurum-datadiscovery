/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package analysis.modules;

import java.util.Set;

public class Entities {

	private Set<String> entities;
	
	public Entities(Set<String> entities) {
		this.entities = entities;
	}
	
	public Set<String> getEntities() {
		return entities;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("[");
		for(String s : entities) {
			sb.append(s);
			sb.append(" - ");
		}
		sb.append("]");
		
		return sb.toString();
	}

}
