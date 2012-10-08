package org.cloudbus.cloudsimgoodies.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

import org.apache.commons.io.output.NullOutputStream;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * Replaces the primitive functionality of the standard Log.
 * 
 * @author Nikolay Grozev
 * 
 *         Adapted from versions of:
 *         <ol>
 *         <li>Anton Beloglazov</li>
 *         <li>William Voorsluys</li>
 *         <li>Adel Nadjaran Toosi</li>
 *         </ol>
 * 
 * @since CloudSim Toolkit 2.0
 */
public class CustomLog {

	public static final String LOG_LEVEL_PROP_KEY = "LogLevel";
	public static final String LOG_CLOUD_SIM_CLOCK_PROP_KEY = "LogCloudSimClock";
	public static final String LOG_FORMAT_PROP_KEY = "LogFormat";
	public static final String FILE_PATH_PROP_KEY = "FilePath";
	public static final String LOG_IN_FILE_PROP_KEY = "LogInFile";
	public static final String SHUT_STANDART_LOGGER_PROP_KEY = "ShutStandardLogger";

	public static final String NEW_LINE = System.getProperty("line.separator");
	private static final String PATTERN = "yyyy-MM-dd_HH:mm:ss";

	private static Level DEFAULT_LEVEL = Level.INFO;

	private static Logger logger;
	private static Level granularityLevel;
	private static Formatter formatter;

	/**
	 * Prints the message passed as a non-String object.
	 * 
	 * @param message
	 *            the message
	 */
	public static void print(Object message, Level level) {
		logger.log(
				level == null ? DEFAULT_LEVEL : level,
				"[" + Double.toString(CloudSim.clock()) + "] "
						+ String.valueOf(message));
	}

	public static String formatDate(Date cal) {
		return new SimpleDateFormat(PATTERN).format(cal);
	}

	/**
	 * Sets the output.
	 * 
	 * @param output
	 *            the new output
	 */
	public static void setOutput(OutputStream output) {
		logger.addHandler(new StreamHandler(output, formatter));
	}

	public static String formatClockTime() {
		DecimalFormat f = new DecimalFormat("#.##");
		return "[Simulation time: " + f.format(CloudSim.clock()) + "]: ";
	}

	public static void configLogger(final Properties props)
			throws SecurityException, IOException {
		if (logger == null) {
			final boolean logInFile = Boolean.parseBoolean(props.getProperty(
					LOG_IN_FILE_PROP_KEY, "false").toString());
			final String fileName = logInFile ? props.getProperty(
					FILE_PATH_PROP_KEY).toString() : null;
			final String format = props.getProperty(LOG_FORMAT_PROP_KEY,
					"getLevel;getMessage").toString();
			final boolean prefixCloudSimClock = Boolean.parseBoolean(props
					.getProperty(LOG_CLOUD_SIM_CLOCK_PROP_KEY, "false")
					.toString());
			final boolean shutStandardMessages = Boolean.parseBoolean(props
					.getProperty(SHUT_STANDART_LOGGER_PROP_KEY, "false")
					.toString());
			granularityLevel = Level.parse(props.getProperty(
					LOG_LEVEL_PROP_KEY, "FINE").toString());

			if (shutStandardMessages) {
				Log.setOutput(new NullOutputStream());
			}

			logger = Logger.getLogger(CustomLog.class.getPackage().getName());
			logger.setUseParentHandlers(false);

			formatter = new CustomFormatter(prefixCloudSimClock, format);

			if(logInFile){
				System.err.println("Rediricting output to " + new File(fileName).getAbsolutePath());
			}
			
			StreamHandler handler = logInFile ? new FileHandler(fileName, false)
					: new ConsoleHandler();
			handler.setLevel(granularityLevel);
			handler.setFormatter(formatter);
			logger.addHandler(handler);
			logger.setLevel(granularityLevel);
		}
	}

	public static boolean isDisabled() {
		return logger.getLevel().equals(Level.OFF);
	}

	public static void printLine(String msg, Level level) {
		logger.log(level == null ? DEFAULT_LEVEL : level, msg);
	}

	private static class CustomFormatter extends Formatter {

		private boolean prefixCloudSimClock;
		private String format;

		public CustomFormatter(boolean prefixCloudSimClock, String format) {
			super();
			this.prefixCloudSimClock = prefixCloudSimClock;
			this.format = format;
		}

		@Override
		public String format(LogRecord record) {
			String[] methodCalls = format.split(";");
			StringBuffer result = new StringBuffer();
			if (prefixCloudSimClock) {
				result.append(formatClockTime());
			}

			int i = 0;
			for (String method : methodCalls) {
				try {
					result.append(record.getClass().getMethod(method)
							.invoke(record));
				} catch (Exception e) {
					System.err.println("Error in logging:");
					e.printStackTrace(System.err);
					System.exit(1);
				}
				if (++i < methodCalls.length - 1) {
					result.append(" -:- ");
				}
			}
			result.append(NEW_LINE);

			return result.toString();
		}
	}
}
