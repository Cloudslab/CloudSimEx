package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.geolocation.IGeolocationService;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

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

    private static final Function<WebBroker, List<WebSession>> EMPTY_LIST_FUNCTION = new Function<WebBroker, List<WebSession>>() {
	@Override
	public List<WebSession> apply(final WebBroker input) {
	    return new ArrayList<>();
	}
    };

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
	Collections.sort(brokers, null);

	// A table of assignments of web sessions to brokers/clouds.
	Map<WebBroker, List<WebSession>> assignments = Maps.asMap(new LinkedHashSet<>(brokers), EMPTY_LIST_FUNCTION);

	// Decide which broker/cloud will serve each session - populate the
	// assignments table accordingly
	for (WebSession sess : webSessions) {
	    List<WebBroker> eligibleBrokers = filterBrokers(brokers, sess, appId);

	    WebBroker selectedBroker = null;
	    for (WebBroker eligibleBroker : eligibleBrokers) {
		ILoadBalancer balancer = eligibleBroker.getLoadBalancers().get(appId);
		if (balancer != null) {
		    selectedBroker = eligibleBroker;
		    String ip = balancer.getIp();
		    String clientIP = sess.getSourceIP();
		    if (geoService.latency(ip, clientIP) < latencySLA) {
			break;
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
	    broker.submitSessionsDirectly(sessions, appId);
	}
    }

    private List<WebBroker> filterBrokers(final List<WebBroker> brokers2, final WebSession sess, final long appId) {
	// TODO Auto-generated method stub
	return null;
    }

}
