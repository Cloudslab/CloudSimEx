package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.geolocation.IGeolocationService;
import org.cloudbus.cloudsim.ex.util.CustomLog;
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

    private final Comparator<WebBroker> costComparator;

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
	    Collections.sort(eligibleBrokers, costComparator);

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
		CustomLog.print("Session " + sess.getSessionId() + " has been denied service.");
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
	    for(WebSession sess : sessions) {
		CustomLog.printf("Session %d will be assigned to %s", sess.getSessionId(), broker.toString());
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

	public CloudPriceComparator(final long appId) {
	    super();
	    this.appId = appId;
	}

	@Override
	public int compare(final WebBroker b1, final WebBroker b2) {
	    ILoadBalancer lb1 = b1.getLoadBalancers().get(appId);
	    ILoadBalancer lb2 = b2.getLoadBalancers().get(appId);

	    BigDecimal cost1 = b1.getVMBillingPolicy().normalisedCostPerMinute(lb1.getAppServers().get(0));
	    BigDecimal cost2 = b2.getVMBillingPolicy().normalisedCostPerMinute(lb2.getAppServers().get(0));

	    return cost1.compareTo(cost2);
	}
    }
}
