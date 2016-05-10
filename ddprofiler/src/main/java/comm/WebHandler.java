/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package comm;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/dd")
public class WebHandler {
	
    @GET
    @Path("test")
    @Produces(MediaType.TEXT_PLAIN)
    public String test() {
        return "Hello World";
    }
    
    @GET
    @Path("new_data_source")
    @Produces(MediaType.APPLICATION_JSON)
    public String newDataSource() {
    	return "";
    }
    
    @GET
    @Path("new_attribute")
    @Produces(MediaType.APPLICATION_JSON)
    public String newAttribute() {
    	return "";
    }
    
}
