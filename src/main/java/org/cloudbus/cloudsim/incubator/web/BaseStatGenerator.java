package org.cloudbus.cloudsim.incubator.web;

import java.util.LinkedList;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.uncommons.maths.number.NumberGenerator;

/**
 * A base class containing the main functionality of a statistical generators of
 * CloutLets. That is this class generates CloudLets based on the provided
 * statistical distributions for their properties.
 * 
 * @author nikolay.grozev
 * 
 * @param <T>
 *            - the actual type of the generated CloudLets.
 */
public abstract class BaseStatGenerator<T extends Cloudlet> implements IGenerator<T> {

    /** A key for the statistical generator of CPU length of the cloudlet. */
    public static final String CLOUDLET_LENGTH = "CLOUDLET_MIS";
    /** A key for the statistical generator of RAM length of the cloudlet. */
    public static final String CLOUDLET_RAM = "CLOUDLET_RAM";
    /** A key for the statistical generator of IO length of the cloudlet. */
    public static final String CLOUDLET_IO = "CLOUDLET_IO";

    protected Map<String, NumberGenerator<Double>> seqGenerators;
    private LinkedList<Double> idealStartUpTimes = new LinkedList<>();
    private double startTime = -1;
    private double endTime = -1;
    private T peeked;

    /**
     * Creates a new generator with the specified statistical distributions.
     * 
     * @param randomGenerators
     *            - the statistical distributions to be used for the generation
     *            of CloudLets. See the standard keys provided above to see what
     *            is usually expected from this map. Inheriting classes may
     *            define their own key and values to be used in the factory
     *            method. Must not be null.
     */
    public BaseStatGenerator(final Map<String, NumberGenerator<Double>> randomGenerators) {
	this(randomGenerators, -1, -1);
    }

    /**
     * Creates a new generator with the specified statistical distributions.
     * 
     * @param randomGenerators
     *            - the statistical distributions to be used for the generation
     *            of CloudLets. See the standard keys provided above to see what
     *            is usually expected from this map. Inheriting classes may
     *            define their own key and values to be used in the factory
     *            method. Must not be null.
     * @param startTime
     *            - the start time of the generation. If positive, no web
     *            cloudlets with ideal start time before this will be generated.
     * @param startTime
     *            - the end time of the generation. If positive, no web
     *            cloudlets with ideal start time after this will be generated.
     */
    public BaseStatGenerator(final Map<String, NumberGenerator<Double>> seqGenerators, final double startTime,
	    final double endTime) {
	this.seqGenerators = seqGenerators;
	this.startTime = startTime;
	this.endTime = endTime;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.IGenerator#peek()
     */
    @Override
    public T peek() {
	if (peeked == null && !idealStartUpTimes.isEmpty()) {
	    peeked = create(idealStartUpTimes.poll());
	}
	return peeked;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.IGenerator#poll()
     */
    @Override
    public T poll() {
	T result = peeked;
	if (peeked != null) {
	    peeked = null;
	} else if (!idealStartUpTimes.isEmpty()) {
	    result = create(idealStartUpTimes.poll());
	}
	return result;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.IGenerator#isEmpty()
     */
    @Override
    public boolean isEmpty() {
	return peek() == null;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.IGenerator#notifyOfTime(double)
     */
    @Override
    public void notifyOfTime(final double time) {
	if ((startTime < 0 || startTime <= time) &&
		(endTime < 0 || endTime >= time) &&
		(idealStartUpTimes.isEmpty() || idealStartUpTimes.getLast() < time)) {
	    idealStartUpTimes.offer(time);
	}
    }

    /**
     * Returns the start time of this generator. If positive, no web cloudlets
     * with ideal start time before this will be generated.
     * 
     * @return the start time of the generator.
     */
    public double getStartTime() {
	return startTime;
    }

    /**
     * Returns the end time of this generator. If positive, no web cloudlets
     * with ideal start time after this will be generated.
     * 
     * @return the end time of the generator.
     */
    public double getEndTime() {
	return endTime;
    }

    /**
     * An abstract factory method for the creation of a new cloudlet. It uses
     * the provided in the constructor statistical distributions to create a new
     * cloudlet.
     * 
     * @param idealStartTime
     *            - the ideal start time of the new CloudLet.
     * @return a new CloudLet.
     */
    protected abstract T create(final double idealStartTime);

    
    /**
     * Generates a plausible value for the key.
     * @param key - the key. Tytpically one of the constants of the class..
     * @return a plausible (with the correspondent statistical properties) value for the key.
     */
    protected Double generateValue(final String key) {
	return !seqGenerators.containsKey(key) ? 0 :
		Math.max(0, seqGenerators.get(key).nextValue());
    }
}
