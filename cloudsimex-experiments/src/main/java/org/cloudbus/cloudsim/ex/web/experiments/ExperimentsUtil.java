package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.cloudbus.cloudsim.ex.util.CustomLog;
import static org.cloudbus.cloudsim.Consts.*;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentsUtil {


    public static final Integer[] HOURS = new Integer[25];

    static {
	for (int i = 0; i <= 24; i++) {
	    HOURS[i] = i * HOUR;
	}
    }

    private ExperimentsUtil() {
	super();
    }

    /**
     * Parses the experiments parameters and sets up the logger.
     * 
     * @param args
     * @throws IOException
     */
    public static void parseExperimentParameters(final String[] args) throws IOException {
	Properties props = new Properties();
	try (InputStream is = Files.newInputStream(Paths.get(args[1]))) {
	    props.load(is);
	}
	props.put(CustomLog.FILE_PATH_PROP_KEY, args[0]);
	CustomLog.configLogger(props);
    }

}
