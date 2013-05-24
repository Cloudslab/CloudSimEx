package org.cloudbus.cloudsim.ex.mapreduce;

/**
 * This class contains the parameters from the simulation that are customizable
 * by users. They are defined in a file called simulation.properties that has to
 * be in Java's classpath.
 * 
 * For adding more properties, follow these steps: 1. Add the property as an
 * enum. The string associated to it represents its key (the string use in the
 * properties file); 2. Add the key and a desirable value in the properties
 * file; 3. read it somewhere in the code (this is a string) as:
 * Properties.NAME.getProperty(); 4. make the approprriated conversion to the
 * desirable type.
 * 
 */

public enum Properties {

    CLOUD("cloud.file"),
    EXPERIMENT("experiment.files");

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
	configuration.setProperty(this.key, value);
    }
}
