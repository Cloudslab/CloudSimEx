package org.cloudbus.cloudsim.workflow;
/**
 * This class contains the parameters from the simulation that
 * are customizable by users. They are defined in a file called
 * simulation.properties that has to be in Java's classpath.
 * 
 * For adding more properties, follow these steps:
 * 1. Add the property as an enum. The string associated to it
 *    represents its key (the string use in the properties file);
 * 2. Add the key and a desirable value in the properties file;
 * 3. read it somewhere in the code (this is a string) as:
 *    Properties.NAME.getProperty();
 * 4. make the approprriated conversion to the desirable type.
 * 
 */

public enum Properties {
	
	EXPERIMENT_ROUNDS("simulation.rounds"),
	HOSTS_PERDATACENTER("datacenter.hosts"),
	VM_DELAY("vm.delay"),
	VM_OFFERS("vm.offers"),
	CORES_PERHOST("host.cores"),
	MEMORY_PERHOST("host.memory"),
	STORAGE_PERHOST("host.storage"),
	MIPS_PERCORE("core.mips"),
	NETWORK_LATENCY("network.latency"),
	INTERNAL_LATENCY("internal.latency"),
	INTERNAL_BANDWIDTH("internal.bandwidth"),
	SCHEDULING_POLICY("scheduling.policy"),
	DAG_FILE("dag.file"),
	DAG_DEADLINE("dag.deadline"),
	REPLICATION_ENABLED("enable.replication"),
	REPLICATION_BUDGET("replication.budget");
	
	private String key;
	private Configuration configuration = Configuration.INSTANCE;
	
	Properties(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public String getProperty() {
		return configuration.getProperty(this.key);
	}
	
	public void setProperty(String value) {
		configuration.setProperty(this.key,value);
	}
}
