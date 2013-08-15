package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import javax.annotation.Nullable;

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

    private final List<WebBroker> brokers = new ArrayList<>();
    private final IGeolocationService geoService;

    public EntryPoint(IGeolocationService geoService) {
	super();
	this.geoService = geoService;
    }

    public void dispatchSessions(final List<WebSession> webSessions, final long appId) {

	Collections.sort(brokers, null);
	Maps.asMap(new LinkedHashSet<>(brokers), EMPTY_LIST_FUNCTION);

	for (WebBroker broker : brokers) {
	    ILoadBalancer balancer = broker.getLoadBalancers().get(appId);
	    if (balancer != null) {
		String ip = balancer.getIp();
		
	    }
	}
    }

}
