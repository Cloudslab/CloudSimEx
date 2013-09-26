package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.geolocation.IGeolocationService;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.vm.VMStatus;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;

/**
 * Represents an entry point for an application, which uses multiple clouds
 * (Multi-Cloud). Each entry point is associated with a list of web brokers,
 * representing the different cloud data centres. The entry point receives
 * workloads (web sessions) and forwards them accordingly to the registered web
 * brokers.
 * 
 * @author nikolay.grozev
 * 
 */
public class EntryPoint {

    private static final double OVERLOAD_UTIL = 0.8;

    private final CloudPriceComparator costComparator;

    private final List<WebSession> canceledSessions = new ArrayList<>();
    private final List<WebBroker> brokers = new ArrayList<>();
    private final IGeolocationService geoService;
    private final long appId;
    private final double latencySLA;

    /**
     * Constr.
     * 
     * @param geoService
     *            - provides the IP utilities needed by the entry point. Must
     *            not be null.
     * @param appId
     *            - the id of the application this entry point services. Must
     *            not be null.
     * @param latencySLA
     *            - the latency SLA of the application.
     */
    public EntryPoint(final IGeolocationService geoService, final long appId, final double latencySLA) {
	this.geoService = geoService;
	this.appId = appId;
	this.latencySLA = latencySLA;

	costComparator = new CloudPriceComparator(appId);
    }

    /**
     * Registers a broker/cloud to this entry point. Subsequently the entry
     * point will consider it when distributing the workload among the
     * registered clouds.
     * 
     * @param broker
     *            - the broker to register. Must not be null.
     */
    public void registerBroker(final WebBroker broker) {
	if (!brokers.contains(broker)) {
	    brokers.add(broker);
	    broker.addEntryPoint(this);
	}
    }

    /**
     * Deregisters the broker/cloud from this entry point. Subsequent workload
     * coming to this entry point will not be distributed to this cloud.
     * 
     * @param webBroker
     *            - the broker to deregister/remove. Must not be null.
     */
    public void deregisterBroker(final WebBroker webBroker) {
	brokers.remove(webBroker);
	webBroker.removeEntryPoint(this);
    }

    /**
     * Returns the id of the application, which this entry point is a part of.
     * 
     * @return - the Id of the application, which this entry point is a part of.
     */
    public long getAppId() {
	return appId;
    }

    /**
     * Dispatches the sessions to the appropriate brokers/clouds.
     * 
     * @param webSessions
     *            - the web sessions to dispatch. Must not be null.
     */
    public void dispatchSessions(final List<WebSession> webSessions) {
	costComparator.prepareToCompare();
	Collections.sort(brokers, costComparator);

	// A table of assignments of web sessions to brokers/clouds.
	Map<WebBroker, List<WebSession>> assignments = new HashMap<>();
	for (WebBroker broker : brokers) {
	    assignments.put(broker, new ArrayList<WebSession>());
	}

	// Decide which broker/cloud will serve each session - populate the
	// assignments table accordingly
	for (WebSession sess : webSessions) {
	    List<WebBroker> eligibleBrokers = filterBrokers(brokers, sess, appId);
	    // Collections.sort(eligibleBrokers, costComparator);

	    WebBroker selectedBroker = null;
	    double bestLatencySoFar = Double.MAX_VALUE;
	    for (WebBroker eligibleBroker : eligibleBrokers) {
		ILoadBalancer balancer = eligibleBroker.getLoadBalancers().get(appId);
		if (balancer != null) {
		    String ip = balancer.getIp();
		    String clientIP = sess.getSourceIP();
		    double latency = geoService.latency(ip, clientIP);

		    if (latency < latencySLA) {
			selectedBroker = eligibleBroker;
			break;
		    } else if (bestLatencySoFar > latency) {
			selectedBroker = eligibleBroker;
			bestLatencySoFar = latency;
		    }
		}
	    }

	    if (selectedBroker == null) {
		CustomLog.print("[Entry Point] Session " + sess.getSessionId() + " has been denied service.");
		canceledSessions.add(sess);
	    } else {
		assignments.get(selectedBroker).add(sess);
		sess.setServerIP(selectedBroker.getLoadBalancers().get(appId).getIp());
	    }
	}

	// Submit the sessions to the selected brokers/clouds
	for (Map.Entry<WebBroker, List<WebSession>> entry : assignments.entrySet()) {
	    WebBroker broker = entry.getKey();
	    List<WebSession> sessions = entry.getValue();
	    for (WebSession sess : sessions) {
		CustomLog.printf("[Entry Point] Session %d will be assigned to %s", sess.getSessionId(),
			broker.toString());
	    }
	    broker.submitSessionsDirectly(sessions, appId);
	}
    }

    private List<WebBroker> filterBrokers(final List<WebBroker> brokers2, final WebSession sess, final long appId) {
	List<WebBroker> eligibleBrokers = new ArrayList<>();
	for (WebBroker b : brokers2) {
	    if (sess.getMetadata() != null && sess.getMetadata().length > 0 &&
		    b.getMetadata() != null && b.getMetadata().length > 0 &&
		    sess.getMetadata()[0].equals(b.getMetadata()[0])) {
		eligibleBrokers.add(b);
	    }
	}
	return eligibleBrokers;
    }

    private static class CloudPriceComparator implements Comparator<WebBroker> {
	private long appId;

	// We do not want to call getASServersToNumSessions() all the time,
	// since it can be resource intensive operation. Thus we cache the
	// values.
	private Map<WebBroker, Map<Integer, Integer>> brokersToMaps = new HashMap<>();
	private Map<WebBroker, Boolean> overloadedDBLayer = new HashMap<>();

	public CloudPriceComparator(final long appId) {
	    super();
	    this.appId = appId;
	}

	/**
	 * Should be called before this comparator is used to sort a collection.
	 */
	public void prepareToCompare() {
	    brokersToMaps.clear();
	    overloadedDBLayer.clear();
	}

	@Override
	public int compare(final WebBroker b1, final WebBroker b2) {
	    return Double.compare(definePrice(b1), definePrice(b2));
	}

	public double definePrice(final WebBroker b) {
	    if (isDBLayerOverloaded(b)) {
		return Double.MAX_VALUE;
	    } else {
		ILoadBalancer lb = b.getLoadBalancers().get(appId);
		BigDecimal pricePerMinute = b.getVMBillingPolicy().normalisedCostPerMinute(lb.getAppServers().get(0));
		Map<Integer, Integer> srvToNumSessions = brokersToMaps.get(b);
		if (srvToNumSessions == null) {
		    srvToNumSessions = b.getASServersToNumSessions();
		    brokersToMaps.put(b, srvToNumSessions);
		}

		int numRunning = 0;
		double sumAvg = 0;
		for (HddVm vm : lb.getAppServers()) {
		    if (vm.getStatus() == VMStatus.RUNNING && srvToNumSessions.containsKey(vm.getId())) {
			numRunning++;
			sumAvg += Math.min(vm.getCPUUtil() / srvToNumSessions.get(vm.getId()), vm.getCPUUtil()
				/ srvToNumSessions.get(vm.getId()));
		    }
		}

		double avgSessionsPerVm = numRunning == 0 ? 1 : 1 / (sumAvg / numRunning);
		return pricePerMinute.doubleValue() * avgSessionsPerVm;
	    }
	}

	private boolean isDBLayerOverloaded(WebBroker b) {
	    if (overloadedDBLayer.containsKey(b)) {
		return overloadedDBLayer.get(b);
	    } else {
		boolean result = true;
		ILoadBalancer lb = b.getLoadBalancers().get(appId);
		for (HddVm db : lb.getDbBalancer().getVMs()) {
		    if (db.getStatus() == VMStatus.RUNNING && db.getCPUUtil() < OVERLOAD_UTIL
			    && db.getRAMUtil() < OVERLOAD_UTIL && db.getDiskUtil() < OVERLOAD_UTIL) {
			result = false;
			break;
		    }
		}
		overloadedDBLayer.put(b, result);

		if (result) {
		    CustomLog.printf("[Entry Point] Broker (%s) has overloaded DB layer", b);
		}

		return result;
	    }
	}
    }
}
