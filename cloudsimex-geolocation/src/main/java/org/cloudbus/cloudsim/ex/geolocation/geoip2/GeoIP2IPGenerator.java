package org.cloudbus.cloudsim.ex.geolocation.geoip2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;

import org.cloudbus.cloudsim.ex.geolocation.BaseIPGenerator;
import org.cloudbus.cloudsim.ex.geolocation.IPGenerator;
import org.cloudbus.cloudsim.ex.geolocation.IPUtil;
import org.cloudbus.cloudsim.ex.util.CustomLog;

import au.com.bytecode.opencsv.CSVReader;

/**
 * An IP generator that generates IPv4 IPs, based on the IP ranges specified in
 * the CSV format provided by <a href="http://www.maxmind.com">GeoLite2</a>.
 * 
 * @author nikolay.grozev
 * 
 */
public class GeoIP2IPGenerator extends BaseIPGenerator implements IPGenerator {

    // TODO Extract these CSV constants elsewhere as they can be reused ...
    /** The separator in the csv file. */
    private static final char CSV_SEP = ',';
    /** The quote symbol in the csv and tsv file. */
    private static final char QUOTE_SYMBOL = '\"';

    /** The CSV file in the GeoLite2 format. */
    private File originalFile;
    /** All ranges specified in the file. */
    private final List<IPRange> ranges = new ArrayList<>();
    /**
     * A list with the same size as {@link GeoIP2IPGenerator.ranges}. Each i-th
     * element in this list contains the sums of the lengths of the ranges in
     * ranges[0:i].
     */
    private final List<Long> accumRangeLengths = new ArrayList<>();
    /** A sum of the lengths of all ranges in the CSV file. */
    private long sumOfRangesLengths = 0;

    /**
     * Constr.
     * 
     * @param countryCodes
     *            - the country codes for this generator.
     * @param f
     *            - a valid CSV file as defined by the GeoLite2 format.
     * @param seed
     *            - a seed if we need to get the same behavior again and again.
     */
    public GeoIP2IPGenerator(final Set<String> countryCodes, final File f, final long seed) {
	super(countryCodes, seed);
	originalFile = f;
	parseFile();
    }

    /**
     * Constr.
     * 
     * @param countryCodes
     *            - the country codes for this generator.
     * @param f
     *            - a valid CSV file as defined by the GeoLite2 format.
     */
    public GeoIP2IPGenerator(final Set<String> countryCodes, final File f) {
	super(countryCodes);
	originalFile = f;
	parseFile();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.ex.geolocation.IPGenerator#pollRandomIP()
     */
    @Override
    public String pollRandomIP() {
	long serachAccum = (long) (getRandom().nextDouble() * sumOfRangesLengths);
	int idx = Collections.binarySearch(accumRangeLengths, serachAccum);
	idx = idx >= 0 ? idx : -idx - 1;
	idx = idx >= ranges.size() ? ranges.size() - 1 : idx;

	IPRange range = ranges.get(idx);

	int ip = range.from + getRandom().nextInt(range.to - range.from);
	return IPUtil.convertIPv4(ip);
    }

    private void parseFile() {
	CustomLog.printf(Level.FINER, "Creating an IP generator from %s for countries %s",
		originalFile.getAbsolutePath(), Arrays.toString(getCountryCodes().toArray()));

	long accum = 0;
	ranges.clear();
	sumOfRangesLengths = 0;

	try (BufferedReader reader = new BufferedReader(new FileReader(originalFile));
		CSVReader csv = new CSVReader(reader, CSV_SEP, QUOTE_SYMBOL)) {
	    // Read the file line by line
	    int lineCount = 0;
	    String[] lineElems = csv.readNext();
	    while ((lineElems = csv.readNext()) != null) {
		String countryCode = lineElems[4];
		if (getCountryCodes().contains(countryCode)) {
		    int from = (int) Long.parseLong(lineElems[2]);
		    int to = (int) Long.parseLong(lineElems[3]);
		    accum += to - from;
		    ranges.add(new IPRange(from, to));
		    accumRangeLengths.add(accum);
		}
		if (++lineCount % 10000 == 0) {
		    CustomLog.printf(Level.FINER, "%d lines processed from %s",
			    lineCount, originalFile.getAbsolutePath());
		}
	    }
	    CustomLog.printf(Level.FINER, "IP generator from %s for countries %s has %d IP ranges",
		    originalFile.getAbsolutePath(), Arrays.toString(getCountryCodes().toArray()), ranges.size());

	    sumOfRangesLengths = accum;
	} catch (IOException e) {
	    ranges.clear();
	    String msg = "File: " + Objects.toString(originalFile.getAbsoluteFile())
		    + " could not be found or read properly. Message: " + e.getMessage();
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException(msg, e);
	}
    }

    /**
     * Represents an IP range
     * 
     * @author nikolay.grozev
     * 
     */
    private static class IPRange {
	private int from;
	private int to;

	public IPRange(int from, int to) {
	    super();
	    this.from = from;
	    this.to = to;
	}

	@Override
	public String toString() {
	    return String.format("[$d,%d]", from, to);
	}
    }
}
