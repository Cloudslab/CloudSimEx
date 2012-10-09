package org.cloudbus.cloudsimgoodies.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;

public final class Id {

	private static final Map<Class<?>, Integer> COUNTERS = new LinkedHashMap<Class<?>, Integer>();
	private static int globalCounter = 0;

	static {
		COUNTERS.put(Cloudlet.class, 1);
		COUNTERS.put(Vm.class, 1);
		COUNTERS.put(Host.class, 1);
		COUNTERS.put(DatacenterBroker.class, 1);
	}

	private Id() {
	}

	public static synchronized int pollId(Class<?> clazz) {
		Class<?> matchClazz = null;
		if (COUNTERS.containsKey(clazz)) {
			matchClazz = clazz;
		} else {
			for (Class<?> key : COUNTERS.keySet()) {
				if (key.isAssignableFrom(clazz)) {
					matchClazz = clazz;
					break;
				}
			}
		}

		if (matchClazz == null) {
			throw new IllegalArgumentException(clazz.getName()
					+ " is not known to the ID generator.");
		}

		int result = COUNTERS.get(matchClazz);
		COUNTERS.put(matchClazz, result + 1);
		return result;
	}

	public static synchronized int pollGlobalId() {
		return globalCounter++;
	}

}
