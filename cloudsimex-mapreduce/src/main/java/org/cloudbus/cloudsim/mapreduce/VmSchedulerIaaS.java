package org.cloudbus.cloudsim.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.lists.PeList;

/**
 * This class implements a VmScheduler that presents heterogeneous performance, i.e., sometimes
 * the amount of  processing allocated to VMs is smaller than the required. Performance loss
 * is modelled after ''Performance Analysis of High Performance Computing Applications on the
 * Amazon Web Services Cloud'' by Jackson et al:
 * 
 * Jackson, K. R.; Ramakrishnan, L.; Muriki, K.; Canon, S.; Cholia, S.; Shalf, J.;
 * Wasserman, H. J.; Wright, N. J. "Performance Analysis of High Performance Computing Applications
 * on the Amazon Web Services Cloud." In: 2nd International Conference on Cloud Computing Technology
 * and Science (CloudCom'10), 2010.
 *
 */
public class VmSchedulerIaaS extends VmSchedulerTimeSharedOverSubscription {
	
	Random random;
	
	/**
	 * Instantiates a new vm scheduler time shared over subscription.
	 * 
	 * @param pelist the pelist
	 */
	public VmSchedulerIaaS(List<? extends Pe> pelist, long seed) {
		super(pelist);
		random = new Random(seed);
	}
	
	@Override
	protected boolean allocatePesForVm(String vmUid, List<Double> mipsShareRequested) {
		double totalRequestedMips = 0;

		// if the requested mips is bigger than the capacity of a single PE, we cap
		// the request to the PE's capacity
		List<Double> mipsShareRequestedCapped = new ArrayList<Double>();
		double peMips = getPeCapacity();
		for (Double mips : mipsShareRequested) {
			if (mips > peMips) {
				mipsShareRequestedCapped.add(peMips);
				totalRequestedMips += peMips;
			} else {
				mipsShareRequestedCapped.add(mips);
				totalRequestedMips += mips;
			}
		}

		getMipsMapRequested().put(vmUid, mipsShareRequested);
		setPesInUse(getPesInUse() + mipsShareRequested.size());
		
		double degradation = getPerformanceDegradation();

		if (getAvailableMips() >= totalRequestedMips) {
			List<Double> mipsShareAllocated = new ArrayList<Double>();
			for (Double mipsRequested : mipsShareRequestedCapped) {
				mipsShareAllocated.add(mipsRequested*degradation);
			}

			getMipsMap().put(vmUid, mipsShareAllocated);
			setAvailableMips(getAvailableMips() - totalRequestedMips);
		} else {
			redistributeMipsDueToOverSubscription(degradation);
		}

		return true;
	}
	
	/**
	 * This method recalculates distribution of MIPs among VMs considering eventual shortage of MIPS
	 * compared to the amount requested by VMs.
	 */
	protected void redistributeMipsDueToOverSubscription(double degradation) {
		// First, we calculate the scaling factor - the MIPS allocation for all VMs will be scaled
		// proportionally
		double totalRequiredMipsByAllVms = 0;

		Map<String, List<Double>> mipsMapCapped = new HashMap<String, List<Double>>();
		for (Entry<String, List<Double>> entry : getMipsMapRequested().entrySet()) {

			double requiredMipsByThisVm = 0.0;
			String vmId = entry.getKey();
			List<Double> mipsShareRequested = entry.getValue();
			List<Double> mipsShareRequestedCapped = new ArrayList<Double>();
			double peMips = getPeCapacity();
			for (Double mips : mipsShareRequested) {
				if (mips > peMips) {
					mipsShareRequestedCapped.add(peMips);
					requiredMipsByThisVm += peMips;
				} else {
					mipsShareRequestedCapped.add(mips);
					requiredMipsByThisVm += mips;
				}
			}

			mipsMapCapped.put(vmId, mipsShareRequestedCapped);
			totalRequiredMipsByAllVms += requiredMipsByThisVm;
		}

		double totalAvailableMips = PeList.getTotalMips(getPeList());
		double scalingFactor = totalAvailableMips / totalRequiredMipsByAllVms;

		// Clear the old MIPS allocation
		getMipsMap().clear();

		// Update the actual MIPS allocated to the VMs
		for (Entry<String, List<Double>> entry : mipsMapCapped.entrySet()) {
			String vmUid = entry.getKey();
			List<Double> requestedMips = entry.getValue();

			List<Double> updatedMipsAllocation = new ArrayList<Double>();
			for (Double mips : requestedMips) {
				mips *= degradation;
				mips *= scalingFactor;
	
				updatedMipsAllocation.add(Math.floor(mips));
			}

			// add in the new map
			getMipsMap().put(vmUid, updatedMipsAllocation);
		}

		// As the host is oversubscribed, there is no more available MIPS
		setAvailableMips(0);
	}
	
	/* 
	 * Returns how much % of the capacity is being actually delivered to the VM.
	 * 1.0 means the whole capacity.
	 */
	private double getPerformanceDegradation(){
		/*Jackson experienced up to 30% variability.
		 * So, we will use 15% in average, 10% stddev
		 */
		double performanceLoss = 0.15 + random.nextGaussian()*0.1;
		if(performanceLoss>0.3) performanceLoss=0.3;
		else if (performanceLoss<0.0) performanceLoss=0.0;

		return 1.0-performanceLoss;
	}
}
