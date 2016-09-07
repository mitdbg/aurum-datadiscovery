/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;

import inputoutput.Attribute;
import inputoutput.conn.Connector;

public interface PreAnalysis {

  public void composeConnector(Connector c);
  public DataQualityReport getQualityReport();
  public List<Attribute> getEstimatedDataTypes();
}
