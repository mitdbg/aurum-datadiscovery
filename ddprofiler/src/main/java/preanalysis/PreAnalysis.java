/**
 * @author ra-mit
 */
package preanalysis;

import java.util.List;

import inputoutput.Attribute;
import inputoutput.connectors.Old_Connector;

public interface PreAnalysis {

  public void composeConnector(Old_Connector c);
  public DataQualityReport getQualityReport();
  public List<Attribute> getEstimatedDataTypes();
}
