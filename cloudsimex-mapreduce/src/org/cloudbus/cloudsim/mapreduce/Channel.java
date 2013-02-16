package org.cloudbus.cloudsim.mapreduce;
import java.util.LinkedList;
import java.util.Random;

/**
 * This class represents a channel for transmission of data between VMs.
 * It controls sharing of available bandwidth among DataItems. Relation between
 * Transmission and Channel is the same as Cloudlet and CloudletScheduler,
 * but here we consider only the time shared case, representing a shared
 * channel among different simultaneous DataItem transmissions.
 * 
 */
public class Channel {

	double bandwidth; //in kBps
	private double previousTime;
	Random random;
	boolean hasDegradation;
	
	LinkedList<Transmission> inTransmission;
	LinkedList<Transmission> completed;
	
	public Channel(double bandwidth) {
		this.bandwidth = bandwidth; /*Unit: kBps*/
		this.previousTime = 0.0;
		this.inTransmission = new LinkedList<Transmission>();
		this.completed = new LinkedList<Transmission>();
		this.hasDegradation = false;
	}
	
	public Channel(double bandwidth, long seed) {
		this.bandwidth = bandwidth; /*Unit: kBps*/
		this.previousTime = 0.0;
		this.inTransmission = new LinkedList<Transmission>();
		this.completed = new LinkedList<Transmission>();
		this.random = new Random(seed);
		this.hasDegradation = true;
	}
		
	/**
	 * Updates processing of transmissions taking place in this Channel.
	 * @param currentTime current simulation time (in seconds)
	 * @return delay to next transmission completion or
	 *         Double.POSITIVE_INFINITY if there is no pending transmissions
	 */
	public double updateTransmission(double currentTime){
		double timeSpan = currentTime-this.previousTime;
		double availableBwPerHost = bandwidth/inTransmission.size();
		
		/*As per Jackson et al studies, variation in data transfer
		  times may reach 65%. Therefore, we model the degradation
		  of the network in a uniform distribution with average 30%
		  and stddev 15%. */
		double degradation=0.0;
		if (hasDegradation) degradation = 0.3 + random.nextGaussian()*0.15;
		
		availableBwPerHost*=(1-degradation);
		
		double transmissionProcessed =  timeSpan*availableBwPerHost;
		
		//update transmission and remove completed ones
		LinkedList<Transmission> completedTransmissions = new LinkedList<Transmission>();
		for(Transmission transmission: inTransmission){
			transmission.addCompletedLength(transmissionProcessed);
			if (transmission.isCompleted()){
				completedTransmissions.add(transmission);
				this.completed.add(transmission);
			}	
		}
		this.inTransmission.removeAll(completedTransmissions);
		
		//now, predicts delay to next transmission completion
		double nextEvent = Double.POSITIVE_INFINITY;
		availableBwPerHost = bandwidth/inTransmission.size();
				
		for (Transmission transmission:this.inTransmission){
			double eft = transmission.getLength()/availableBwPerHost;
			if (eft<nextEvent) nextEvent = eft;
		}
		return nextEvent;
	}
	
	/**
	 * Adds a new Transmission to be submitted via this Channel
	 * @param transmission transmission initiating
	 * @return estimated delay to complete this transmission
	 * 
	 */
	public double addTransmission(Transmission transmission){
		this.inTransmission.add(transmission);
		return transmission.getLength()/(bandwidth/inTransmission.size());
	}
	
	/**
	 * Remove a transmission submitted to this Channel
	 * @param transmission to be removed
	 * 
	 */
	public void removeTransmission(Transmission transmission){
		inTransmission.remove(transmission);
	}
	
	/**
	 * @return list of DataItems whose transmission finished, or empty
	 *         list if no dataItem arrived.
	 */
	public LinkedList<Transmission> getArrivedDataItems(){
		LinkedList<Transmission> returnList = new LinkedList<Transmission>();
		
		if (!completed.isEmpty()){
			returnList.addAll(completed);
		}
		completed.removeAll(returnList);
				
		return returnList;
	}
		
	public double getLastUpdateTime(){
		return previousTime;
	}
}
