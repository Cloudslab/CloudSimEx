package org.cloudbus.cloudsim.mapreduce;
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
	VM_DELAY("vm.delay"),
	SCHEDULING_POLICY("scheduling.policy"),
	VM_OFFERS("vm.offers"),
	
	MEMORY_PERHOST("host.memory"),
	STORAGE_PERHOST("host.storage"),
	CORES_PERHOST("host.cores"),
	MIPS_PERCORE("core.mips"),
	
	DATACENTERS_COUNT("datacenter.count"),
	/* It has to be an Array */
	DATACENTER1_NAME("datacenter1.name"),
	DATACENTER1_HOSTS("datacenter1.hosts"),
	
	CORE_PERVM("vm.core"),
	SIZE_PERVM("vm.size"),
	MEMORY_PERVM("vm.ram"),
	PES_PERVM("vm.pesNumber"),
	
	VMS_COUNT("vm.count"),
	/* It has to be an Array */
	VM1_NAME("vm1.name"),
	VM1_COST("vm1.cost"),
	VM1_TRANSFERRING_COST("vm1.transferring.cost"),
	VM1_MIPS("vm1.mips"),
	VM1_AVAILABLERESOURCES("vm1.ar"),
	VM1_DATACENTER("vm1.datacenter"),
	
	DATASOURCES_COUNT("ds.count"),
	/* It has to be an Array */
	DATASOURCE1_NAME("gbr-sensors"),
	DATASOURCE1_COST("0.000"),
	
	REQUESTS_COUNT("request.count"),
	/* It has to be an Array */
	REQUEST1_BUDGET("request1.budget"),
	REQUEST1_DEADLINE("request1.deadline"),
	REQUEST1_JOB_FILE("request1.job"),
	REQUEST1_USERCLASS("request1.class")
	
;
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
