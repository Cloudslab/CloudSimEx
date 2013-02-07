package org.cloudbus.cloudsim.incubator.web.workload.sessions;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.incubator.disk.DataItem;
import org.cloudbus.cloudsim.incubator.web.CompositeGenerator;
import org.cloudbus.cloudsim.incubator.web.IGenerator;
import org.cloudbus.cloudsim.incubator.web.StatGenerator;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.incubator.web.WebSession;
import org.uncommons.maths.number.ConstantGenerator;
import org.uncommons.maths.number.NumberGenerator;

/**
 * Generates equal sessions that consist of equal cloudlets and use only one
 * data item.
 * 
 * @author nikolay.grozev
 * 
 */
public class ConstSessionGenerator implements ISessionGenerator {
    private final long asCloudletLength;
    private final int asRam;

    private final long dbCloudletLength;
    private final int dbRam;
    private final long dbCloudletIOLength;

    private final double duration;
    private int numberOfCloudlets = 0;

    private final DataItem data;

    /**
     * Constructor.
     * 
     * @param asCloudletLength
     *            - the length of the generated app server cloudlets.
     * @param asRam
     *            - the ram of the generated app server cloudlets.
     * @param dbCloudletLength
     *            - the length of the generated db server cloudlets.
     * @param dbRam
     *            - the ram of the generated db server cloudlets.
     * @param dbCloudletIOLength
     *            - the IO ram of the generated app server cloudlets.
     * @param duration
     *            - the duration of the generated sessions.
     * @param numberOfCloudlets
     *            - number of cloudlets in each sessions. Negative means
     *            infinity.
     * @param data
     *            - the data used by the database cloudlets.
     */
    public ConstSessionGenerator(final long asCloudletLength, final int asRam, final long dbCloudletLength,
	    final int dbRam,
	    final long dbCloudletIOLength, final double duration, final int numberOfCloudlets, final DataItem data) {
	super();
	this.asCloudletLength = asCloudletLength;
	this.asRam = asRam;
	this.dbCloudletLength = dbCloudletLength;
	this.dbRam = dbRam;
	this.dbCloudletIOLength = dbCloudletIOLength;
	this.duration = duration;
	this.numberOfCloudlets = numberOfCloudlets;
	this.data = data;
    }

    /**
     * Constructor.
     * 
     * @param asCloudletLength
     *            - the length of the generated app server cloudlets.
     * @param asRam
     *            - the ram of the generated app server cloudlets.
     * @param dbCloudletLength
     *            - the length of the generated db server cloudlets.
     * @param dbRam
     *            - the ram of the generated db server cloudlets.
     * @param dbCloudletIOLength
     *            - the IO ram of the generated app server cloudlets.
     * @param data
     *            - the data used by the database cloudlets.
     */
    public ConstSessionGenerator(final long asCloudletLength, final int asRam, final long dbCloudletLength,
	    final int dbRam,
	    final long dbCloudletIOLength, final DataItem data) {
	this(asCloudletLength, asRam, dbCloudletLength, dbRam, dbCloudletIOLength, -1, -1, data);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.incubator.web.workload.sessions.ISessionGenerator
     * #generateSessionAt(double)
     */
    @Override
    public WebSession generateSessionAt(final double time) {
	double startTime = duration > 0 ? time : -1;
	double endTime = duration > 0 ? time + duration : -1;

	Map<String, NumberGenerator<Double>> asGenerators = new HashMap<>();
	asGenerators.put(StatGenerator.CLOUDLET_LENGTH, new ConstantGenerator<Double>((double) asCloudletLength));
	asGenerators.put(StatGenerator.CLOUDLET_RAM, new ConstantGenerator<Double>((double) asRam));
	IGenerator<WebCloudlet> appServerCloudLets = new StatGenerator(asGenerators, startTime, endTime, null);

	Map<String, NumberGenerator<Double>> dbGenerators = new HashMap<>();
	dbGenerators.put(StatGenerator.CLOUDLET_LENGTH, new ConstantGenerator<Double>((double) dbCloudletLength));
	dbGenerators.put(StatGenerator.CLOUDLET_RAM, new ConstantGenerator<Double>((double) dbRam));
	dbGenerators.put(StatGenerator.CLOUDLET_IO, new ConstantGenerator<Double>((double) dbCloudletIOLength));
	CompositeGenerator<? extends WebCloudlet> dbServerCloudLets =
		new CompositeGenerator<>(new StatGenerator(dbGenerators, startTime, endTime, data));

	return new WebSession(appServerCloudLets, dbServerCloudLets, -1, numberOfCloudlets, time + duration);
    }

}
