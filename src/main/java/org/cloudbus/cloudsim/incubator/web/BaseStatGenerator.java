package org.cloudbus.cloudsim.incubator.web;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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
public abstract class BaseStatGenerator<T extends Cloudlet> implements
	IGenerator<T> {

    /** A key for the statistical generator of CPU length of the cloudlet. */
    public static final String CLOUDLET_LENGTH = "CLOUDLET_MIS";
    /** A key for the statistical generator of RAM length of the cloudlet. */
    public static final String CLOUDLET_RAM = "CLOUDLET_RAM";

    protected Map<String, NumberGenerator<Double>> seqGenerators;
    private Queue<Double> ticks = new LinkedList<>();
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
	this.seqGenerators = randomGenerators;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.IGenerator#peek()
     */
    @Override
    public T peek() {
	if (peeked == null && !ticks.isEmpty()) {
	    peeked = create(ticks.poll());
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
	} else if (!ticks.isEmpty()) {
	    result = create(ticks.poll());
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
	if (ticks.isEmpty() || ticks.peek() < time) {
	    ticks.offer(time);
	}
    }

    /**
     * An abstract factory method for the creation of a new cloudlet. It uses
     * the provided in the constructor statistical distributions to create a new
     * cloudlet.
     * 
     * @param idealStartTime - the ideal start time of the new CloudLet.
     * @return a new CloudLet.
     */
    protected abstract T create(final double idealStartTime);

}
