package org.cloudbus.cloudsim.incubator.web;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.uncommons.maths.number.NumberGenerator;

public abstract class BaseStatGenerator<T extends Cloudlet> implements
		IGenerator<T> {

	public static final String CLOUDLET_LENGTH = "CLOUDLET_MIS";
	public static final String CLOUDLET_RAM = "CLOUDLET_MIS";

	protected Map<String, NumberGenerator<Double>> seqGenerators;
	private Queue<Double> ticks = new LinkedList<>();
	private T peeked;

	public BaseStatGenerator(
			Map<String, NumberGenerator<Double>> randomGenerators) {
		this.seqGenerators = randomGenerators;
	}

	public T peek() {
		if (peeked == null && !ticks.isEmpty()) {
			peeked = create(ticks.poll());
		}
		return peeked;
	}

	public T poll() {
		T result = peeked;
		if (peeked != null) {
			peeked = null;
		} else if (!ticks.isEmpty()) {
			result = create(ticks.poll());
		}
		return result;
	}

	public boolean isEmpty() {
		return peek() == null;
	}

	@Override
	public void prefetch(final double time) {
		ticks.offer(time);
	}

	protected abstract T create(final double idealStartTime);

}
