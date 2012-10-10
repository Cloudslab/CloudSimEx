package org.cloudbus.cloudsim.incubator.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.web.WebSession;

public final class Id {

    private static final Map<Class<?>, Integer> COUNTERS = new LinkedHashMap<>();
    private static int globalCounter = 0;

    static {
	COUNTERS.put(Cloudlet.class, 1);
	COUNTERS.put(Vm.class, 1);
	COUNTERS.put(Host.class, 1);
	COUNTERS.put(DatacenterBroker.class, 1);
	COUNTERS.put(WebSession.class, 1);
    }

    private Id() {
    }

    public static synchronized int pollId(Class<?> clazz) {
	Class<?> matchClass = null;
	if (COUNTERS.containsKey(clazz)) {
	    matchClass = clazz;
	} else {
	    for (Class<?> key : COUNTERS.keySet()) {
		if (key.isAssignableFrom(clazz)) {
		    matchClass = clazz;
		    break;
		}
	    }
	}

	int result = -1;
	if (matchClass == null) {
	    result = pollGlobalId();
	} else {
	    result = COUNTERS.get(matchClass);
	    COUNTERS.put(matchClass, result + 1);
	}
	return result;
    }

    public static synchronized int pollGlobalId() {
	return globalCounter++;
    }

}
