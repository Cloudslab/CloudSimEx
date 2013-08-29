package org.cloudbus.cloudsim.ex.vm;

/**
 * Represents VM metadata - e.g. type, OS etc. All properties can be null.
 * 
 * @author nikolay.grozev
 * 
 */
public class VMMetadata {

    private String type;
    private String os;

    // TODO new properties go here...

    /**
     * Returns the type of the VM.
     * 
     * @return the type of the VM.
     */
    public String getType() {
	return type;
    }

    /**
     * Sets the VM type.
     * 
     * @param type
     *            - the VM type
     */
    public void setType(String type) {
	this.type = type;
    }

    /**
     * Returns the OS.
     * 
     * @return the OS.
     */
    public String getOS() {
	return os;
    }

    /**
     * Sets the os.
     * 
     * @param os
     *            - the os.
     */
    public void setOS(String os) {
	this.os = os;
    }

}
