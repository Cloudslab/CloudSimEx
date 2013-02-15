package org.cloudbus.cloudsim.workflow;

/**
 * This class represents transmission of a DataItem. It controls
 * amount of data transmitted in a shared data. Relation between
 * Transmission and Channel is the same as Cloudlet and CloudletScheduler,
 * but here we consider only the time shared case, representing a shared
 * channel among different simultaneous DataItem transmissions.
 * 
 */
public class Transmission {
	int sourceId;
	int destinationId;
	long totalLength; 	/*length in kB*/
	double leftLength;
	DataItem data;
	
	public Transmission(DataItem data,int sourceId, int destinationId) {
		this.sourceId = sourceId;
		this.destinationId = destinationId;
		this.data = data;
		this.totalLength = data.getSize();
		this.leftLength = totalLength;
	}
	
	/**
	 * Sums some amount of data to the already transmitted data
	 * @param completed amount of data completed since last update
	 */
	public void addCompletedLength(double completed){
		leftLength-=completed;
		if (leftLength<0.1) leftLength = 0.0;
	}
	
	public int getSourceId(){
		return sourceId;
	}
	
	public int getDestinationId(){
		return destinationId;
	}
	
	public DataItem getDataItem() {
		return data;
	}
	
	public double getLength(){
		return leftLength;
	}
	
	/**
	 * Says if the DataItem transmission finished or not.
	 * @return true if transmission finished; false otherwise
	 */
	public boolean isCompleted(){
		return leftLength<0.1;
	}
}
