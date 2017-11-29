package sources;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.Conductor;
import core.TaskPackage;
import core.config.sources.CSVSourceConfig;
import core.config.sources.SourceConfig;

public class CSVSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(CSVSource.class.getName());

    @Override
    public void processSource(SourceConfig config, Conductor c) {
	assert (config instanceof CSVSourceConfig);

	CSVSourceConfig csvConfig = (CSVSourceConfig) config;
	String pathToSources = csvConfig.getPath();

	// TODO: at this point we'll be harnessing metadata from the source

	File folder = new File(pathToSources);
	int totalFiles = 0;
	int tt = 0;

	File[] filePaths = folder.listFiles();
	for (File f : filePaths) {
	    tt++;
	    if (f.isFile()) {
		String path = f.getParent() + File.separator;
		String name = f.getName();

		TaskPackage tp = TaskPackage.makeCSVFileTaskPackage(csvConfig.getSourceName(), path, name,
			csvConfig.getSeparator());

		totalFiles++;
		c.submitTask(tp);
	    }
	}

	LOG.info("Total files submitted for processing: {} - {}", totalFiles, tt);
    }

}
