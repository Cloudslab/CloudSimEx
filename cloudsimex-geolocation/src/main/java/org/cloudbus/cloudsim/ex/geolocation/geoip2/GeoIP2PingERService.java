package org.cloudbus.cloudsim.ex.geolocation.geoip2;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.ex.geolocation.BaseGeolocationService;
import org.cloudbus.cloudsim.ex.geolocation.IGeolocationService;
import org.cloudbus.cloudsim.ex.util.CustomLog;

import au.com.bytecode.opencsv.CSVReader;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.City;

/**
 * 
 * Implements an offline {@link IGeolocationService} from the data downloaded
 * from <a href="http://www.maxmind.com">GeoLite2</a> and <a
 * href="http://www-iepm.slac.stanford.edu/pinger/">PingER</a>.
 * 
 * <br>
 * <br>
 * The PingER data can be extracted with the following request: <a href=
 * "http://pinger.seecs.edu.pk/cgi-bin/pingtable.pl?format=tsv&file=average_rtt&by=by-node&size=1000&tick=monthly&year=2013&month=08&from=WORLD&to=WORLD&ex=none&dataset=hep&percentage=75%25&filter=on"
 * >PingER request URL</a>.
 * 
 * 
 * @author nikolay.grozev
 * 
 */
public class GeoIP2PingERService extends BaseGeolocationService implements IGeolocationService, Closeable {

    /**
     * A latency threshold, below which the latency is considered to be 0, which
     * is unacceptably low.
     */
    private static final double LATENCY_EPSILON = 0.001;
    /** A pattern for a string representing a decimal double number. */
    private static final String DOUBLE_GROUP_PATTERN = "(\\-?\\d+(\\.\\d+)?)";
    /** A regular expression for strings of the format (latitude longitude). */
    private static Pattern COORD_PATTERN =
	    Pattern.compile("\\(\\s*" + DOUBLE_GROUP_PATTERN + "\\s+" + DOUBLE_GROUP_PATTERN + "\\s*\\)");

    private DatabaseReader reader;

    private final Map<String, Double[]> nodesTable = new HashMap<>();
    private final Map<Pair<String, String>, Double> latencyTable = new HashMap<>();

    /**
     * Constructor.
     * 
     * @param geoIP2DB
     *            - a valid file in the mmdb format.
     * @param pingErRTTFile
     *            - a TSV file extracted from the PingER service, containing
     *            data about RTTs between hosts distributed worldwide.
     * @param pingerMonitoringSitesFile
     *            - a CSV file extracted from the PingER service, containing the
     *            metadata of all hosts.
     */
    public GeoIP2PingERService(final File geoIP2DB, final File pingErRTTFile, final File pingerMonitoringSitesFile) {
	super();
	try {
	    reader = new DatabaseReader(geoIP2DB);

	    parsePingER(pingErRTTFile, pingerMonitoringSitesFile);

	} catch (IOException e) {
	    String msg = "Invalid file: " + Objects.toString(geoIP2DB) + " Error details:" + e.getMessage();
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException(msg, e);
	}
    }

    private void parsePingER(final File pingErRTTFile, final File pingerMonitoringSitesFile) {
	try (BufferedReader pingsReader = new BufferedReader(new FileReader(pingErRTTFile));
		BufferedReader nodeDefsReader = new BufferedReader(new FileReader(pingerMonitoringSitesFile))) {
	    parseNodesDefitions(nodeDefsReader);
	    parseInterNodePings(pingsReader);
	} catch (IOException e) {
	    String msg = " A file could not be found or read properly. Message: " + e.getMessage();
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException(msg, e);
	}
    }

    private void parseInterNodePings(final BufferedReader pings) throws IOException {
	latencyTable.clear();
	Set<String> unknownNodes = new LinkedHashSet<>();
	try (CSVReader csv = new CSVReader(pings, '\t', '\"')) {
	    // Skip header line
	    String[] lineElems = csv.readNext();
	    while ((lineElems = csv.readNext()) != null) {
		List<Double> measurements = new ArrayList<>();
		String monitoringNode = null;
		String remoteNode = null;

		for (int i = 3; i < lineElems.length; i++) {
		    String element = lineElems[i].trim();
		    if (element.isEmpty() || element.matches(DOUBLE_GROUP_PATTERN)) {
			double rtt = Double.parseDouble(element);
			// We consider the latency to be half the RTT
			measurements.add(rtt / 2);
		    } else {
			monitoringNode = element;
			remoteNode = lineElems[i + 3].trim();
			break;
		    }
		}

		Double latency = latency(measurements);
		if (!this.nodesTable.containsKey(monitoringNode)) {
		    unknownNodes.add(monitoringNode);
		} else if (!this.nodesTable.containsKey(remoteNode)) {
		    unknownNodes.add(monitoringNode);
		} else if (latency != null) {
		    latencyTable.put(ImmutablePair.of(monitoringNode, remoteNode), latency);
		}
	    }
	}
	CustomLog.print("The definitions of the following nodes are missing." + unknownNodes.toString());
    }

    private void parseNodesDefitions(final BufferedReader defs) throws IOException {
	nodesTable.clear();
	try (CSVReader csv = new CSVReader(defs, ',', '\"')) {
	    // Skip header line
	    String[] lineElems = csv.readNext();
	    while ((lineElems = csv.readNext()) != null) {
		String node = lineElems[0].trim();
		String location = lineElems[2].trim();

		Matcher matcher = COORD_PATTERN.matcher(location);
		if (matcher.find()) {
		    Double lat = Double.parseDouble(matcher.group(1));
		    Double lon = Double.parseDouble(matcher.group(3));
		    nodesTable.put(node, new Double[] { lat, lon });
		} else {
		    nodesTable.clear();
		    throw new IllegalArgumentException("Could not extract the geo location from \"" + location + "\"");
		}
	    }
	}
    }

    private static Double latency(final List<Double> measurements) {
	double sum = 0;
	int count = 0;
	for (Double d : measurements) {
	    if (Math.abs(d - 0) > LATENCY_EPSILON) {
		sum += d;
		count++;
	    }
	}

	return count != 0 ? sum / count : null;
    }

    @Override
    public Double[] getCoordinates(final String ip) {
	City city;
	try {
	    city = reader.city(InetAddress.getByName(ip));
	    return new Double[] { city.getLocation().getLatitude(),
		    city.getLocation().getLongitude() };
	} catch (UnknownHostException e) {
	    String msg = "Invalid IP: " + Objects.toString(ip);
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException("Invalid IP", e);
	} catch (IOException | GeoIp2Exception e) {
	    String msg = "Could not locate IP: " + Objects.toString(ip) + ", because " + e.getMessage();
	    CustomLog.logError(Level.INFO, msg, e);
	    return new Double[] { null, null };
	}
    }

    @Override
    public double latency(final String ip1, final String ip2) {
	final Double[] reqCoord1 = getCoordinates(ip1);
	final Double[] reqCoord2 = getCoordinates(ip2);

	double bestDistance = Double.POSITIVE_INFINITY;
	double bestLatency = 0;
	Double[] bestCoord1 = null;
	Double[] bestCoord2 = null;
	String[] bestNodesName = null;

	for (Map.Entry<Pair<String, String>, Double> el : latencyTable.entrySet()) {
	    Double[] nodeCoord1 = nodesTable.get(el.getKey().getLeft());
	    Double[] nodeCoord2 = nodesTable.get(el.getKey().getRight());

	    if (nodeCoord1 == null || nodeCoord2 == null) {
		continue;
	    }

	    double distance1 = distance(reqCoord1, nodeCoord1);
	    double distance2 = distance(reqCoord2, nodeCoord2);
	    double distanceSum = distance1 + distance2;

	    double distance1Inverse = distance(reqCoord1, nodeCoord2);
	    double distance2Inverse = distance(reqCoord2, nodeCoord1);
	    double distanceSumInverse = distance1Inverse + distance2Inverse;

	    if (distanceSum < distanceSumInverse && distance1 + distance2 < bestDistance) {
		bestDistance = distance1 + distance2;
		bestLatency = el.getValue();
		bestNodesName = new String[] { el.getKey().getLeft(), el.getKey().getRight() };
		bestCoord1 = nodeCoord1;
		bestCoord2 = nodeCoord2;
	    } else if (distance1Inverse + distance2Inverse < bestDistance) {
		bestDistance = distance1Inverse + distance2Inverse;
		bestLatency = el.getValue();
		bestNodesName = new String[] { el.getKey().getLeft(), el.getKey().getRight() };
		bestCoord1 = nodeCoord2;
		bestCoord2 = nodeCoord1;
	    }
	}

	CustomLog.print("Used coordinates for latency estimation: " +
		getLocationMapUrl(bestCoord1) + "\n" + getLocationMapUrl(bestCoord2));
	CustomLog.print("Used nodes for latency estimation: " +
		bestNodesName[0] + "," + bestNodesName[1]);

	return bestLatency;
    }

    @Override
    public String[] getMetaData(final String ip) {
	City city;
	try {
	    city = reader.city(InetAddress.getByName(ip));
	    return new String[] { city.getContinent().getName(),
		    city.getContinent().getCode(),
		    city.getCountry().getName(),
		    city.getCountry().getIsoCode(),
		    city.getCity().getName(),
		    city.getPostal().getCode(),
		    Double.toString(city.getLocation().getLatitude()),
		    Double.toString(city.getLocation().getLongitude()) };
	} catch (UnknownHostException e) {
	    String msg = "Invalid IP: " + Objects.toString(ip);
	    CustomLog.logError(Level.SEVERE, msg, e);
	    throw new IllegalArgumentException("Invalid IP", e);
	} catch (IOException | GeoIp2Exception e) {
	    String msg = "Could not locate IP: " + Objects.toString(ip) + ", because: " + e.getMessage();
	    CustomLog.logError(Level.INFO, msg, e);
	    return new String[8];
	}
    }

    @Override
    public void close() throws IOException {
	reader.close();
    }

    public static void main(String[] args) throws IOException {
	String ip = "190.96.64.0";
	try (GeoIP2PingERService service = new GeoIP2PingERService(new File("GeoLite2-City.mmdb"),
		new File("PingTablePingER.tsv"),
		new File("MonitoringSitesPingER.csv"))) {
	    System.out.println(service.getTxtAddress(ip));
	}
    }

}
