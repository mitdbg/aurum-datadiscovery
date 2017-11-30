package inputoutput.connectors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import core.SourceType;
import inputoutput.Attribute;

public interface Connector {

    public SourceType getSourceType();

    public void initConnector() throws IOException, ClassNotFoundException, SQLException;

    public void destroyConnector();

    public List<Attribute> getAttributes() throws IOException, SQLException;

    public Map<Attribute, List<String>> readRows(int num) throws IOException, SQLException;

}
