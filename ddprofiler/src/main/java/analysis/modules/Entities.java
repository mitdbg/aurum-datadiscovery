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

}
