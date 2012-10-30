package org.cloudbus.cloudsim.incubator.web.workload;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.incubator.web.StatGenerator;
import org.cloudbus.cloudsim.incubator.web.IGenerator;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.incubator.web.WebSession;
import org.uncommons.maths.number.ConstantGenerator;
import org.uncommons.maths.number.NumberGenerator;

/**
 * Generates equal sessions that consist of equal cloudlets.
 * 
 * @author nikolay.grozev
 * 
 */
public class ConstSessionGenerator implements ISessionGenerator {
    private long asCloudletLength;
    private int asRam;

    private long dbCloudletLength;
    private int dbRam;
    private long dbCloudletIOLength;

    private double startTime;
    private double endTime;

    /**
     * Constructor.
     * @param asCloudletLength - the length of the generated app server cloudlets.
     * @param asRam - the ram of the generated app server cloudlets.
     * @param dbCloudletLength - the length of the generated db server cloudlets.
     * @param dbRam - the ram of the generated db server cloudlets.
     * @param dbCloudletIOLength - the IO ram of the generated app server cloudlets.
     * @param startTime - the start time. No cloudlets before this moment will be generated.
     * @param endTime - the end time. No cloudlets after this moment will be generated.
     */
    public ConstSessionGenerator(long asCloudletLength, int asRam, long dbCloudletLength, int dbRam,
	    long dbCloudletIOLength, double startTime, double endTime) {
	super();
	this.asCloudletLength = asCloudletLength;
	this.asRam = asRam;
	this.dbCloudletLength = dbCloudletLength;
	this.dbRam = dbRam;
	this.dbCloudletIOLength = dbCloudletIOLength;
	this.startTime = startTime;
	this.endTime = endTime;
    }

    /**
     * 
     * @param asCloudletLength
     * @param asRam
     * @param dbCloudletLength
     * @param dbRam
     * @param dbCloudletIOLength
     */
    public ConstSessionGenerator(long asCloudletLength, int asRam, long dbCloudletLength, int dbRam,
	    long dbCloudletIOLength) {
	this(asCloudletLength, asRam, dbCloudletLength, dbRam, dbCloudletIOLength, -1, -1);
    }

    @Override
    public WebSession generateSessionAt(double time) {

	Map<String, NumberGenerator<Double>> asGenerators = new HashMap<>();
	asGenerators.put(StatGenerator.CLOUDLET_LENGTH, new ConstantGenerator<Double>((double) asCloudletLength));
	asGenerators.put(StatGenerator.CLOUDLET_RAM, new ConstantGenerator<Double>((double) asRam));
	IGenerator<WebCloudlet> appServerCloudLets = new StatGenerator(asGenerators, startTime, endTime);

	Map<String, NumberGenerator<Double>> dbGenerators = new HashMap<>();
	dbGenerators.put(StatGenerator.CLOUDLET_LENGTH, new ConstantGenerator<Double>((double) dbCloudletLength));
	dbGenerators.put(StatGenerator.CLOUDLET_RAM, new ConstantGenerator<Double>((double) dbRam));
	dbGenerators.put(StatGenerator.CLOUDLET_IO, new ConstantGenerator<Double>((double) dbCloudletIOLength));
	IGenerator<WebCloudlet> dbServerCloudLets = new StatGenerator(dbGenerators, startTime, endTime);

	return new WebSession(appServerCloudLets, dbServerCloudLets, -1);
    }

}
