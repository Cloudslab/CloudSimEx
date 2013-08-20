package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.geolocation.IGeolocationService;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

/**
 * 
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

    private static final double latencySLA = 100;

    private final List<WebBroker> brokers = new ArrayList<>();
    private final IGeolocationService geoService;

    public EntryPoint(IGeolocationService geoService) {
	super();
	this.geoService = geoService;
    }

    /**
     * Dispatches the sessions to the appropriate broker/cloud
     * 
     * @param webSessions
     * @param appId
     */
    public void dispatchSessions(final List<WebSession> webSessions, final long appId) {
	Collections.sort(brokers, null);

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
		// TODO Deny service
	    } else {
		assignments.get(selectedBroker).add(sess);
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
