package org.cloudbus.cloudsim.ex.geolocation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import org.cloudbus.cloudsim.ex.util.CustomLog;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class IPGenerator {

    private static IPAccumulatedWigthsComparator IP_WEIGTHS_COMPARATOR = new IPAccumulatedWigthsComparator();
    
    private Set<String> countryCodes = new HashSet<>();
    private List<IPRange> ranges = new ArrayList<>();
    private long rangeOfSums = 0;
    private File originalFile;
    private Random random = new Random();

    public IPGenerator(final Set<String> countryCodes, final File f, final long seed) {
	this(countryCodes, f);
	random.setSeed(seed);
    }
    
    public IPGenerator(final Set<String> countryCodes, final File f) {
	this.countryCodes = countryCodes;
	originalFile = f;
	parseFile();
    }

    public String pollRandomIP() {
	long serachAccum = (long) (random.nextDouble() * rangeOfSums);
	int idx = Collections.binarySearch(ranges, serachAccum, IP_WEIGTHS_COMPARATOR);
	idx = idx >= 0 ? idx : -idx - 1;
	idx = idx >= ranges.size() ? ranges.size() - 1 : idx;
	
	IPRange range = ranges.get(idx); 
	
	int ip = range.to + random.nextInt(range.to - range.from);
	return convertIP(ip);
    }

    private String convertIP(int ip) {
	/// .... 
	return null;
    }

    private void parseFile() {
	long accum = 0;
	ranges.clear();
	rangeOfSums = 0;
	try (BufferedReader reader = new BufferedReader(new FileReader(originalFile))) {
	    String line = null;
	    while ((line = reader.readLine()) != null) {
		Iterable<String> lineElementsIterable = Splitter.on(",")
			.trimResults(CharMatcher.WHITESPACE.and(CharMatcher.is('\"'))).split(line);
		List<String> lineElements = Lists.newArrayList(lineElementsIterable);
		String countryCode = lineElements.get(4);
		if (countryCodes.contains(countryCode)) {
		    int from = Integer.parseInt(lineElements.get(2));
		    int to = Integer.parseInt(lineElements.get(3));
		    accum += to - from;
		    ranges.add(new IPRange(from, to, accum));
		}
	    }
	    rangeOfSums = accum;
	} catch (IOException e) {
	    ranges.clear();
	    String msg = "File: " + Objects.toString(originalFile) + " could not be found or read properly. Message: "
		    + e.getMessage();
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException(msg, e);
	}

    }

    private static class IPAccumulatedWigthsComparator implements Comparator<Object>{

	@Override
	public int compare(Object o1, Object o2) {
	    long o1Val = extractVal(o1);
	    long o2Val = extractVal(o2);
	    return o1Val == o2Val ? 0
		    : o1Val > o2Val ? 1 : -1;
	}
	
	private long extractVal(final Object o) {
	    if (o instanceof IPRange) {
		return ((IPRange)o).accumSize;
	    } else if(o instanceof Number){
		return ((Number)o).longValue();
	    } else {
		throw new IllegalArgumentException("This comparator only compares IPRanges and numbers");
	    }
	}
	
    }
    
    private static class IPRange {
	private int from;
	private int to;
	private long accumSize;

	public IPRange(int from, int to, long accumSize) {
	    super();
	    this.from = from;
	    this.to = to;
	    this.accumSize = accumSize;
	}
    }

}
