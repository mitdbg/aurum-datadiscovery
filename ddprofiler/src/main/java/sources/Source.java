package sources;

import core.Conductor;
import sources.config.SourceConfig;

public interface Source {

    public void processSource(SourceConfig config, Conductor c);

}
