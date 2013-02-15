package org.cloudbus.cloudsim.workflow;

/**
 * This class contains information necessary to enable data
 * transfer among virtual machines in a data center. It contains
 * the dataItem being transferred, VM origin and VM destination.
 */
public class TransferDataEvent {

	DataItem dataItem;
	int sourceId;
	int destinationId;
	
	public TransferDataEvent(DataItem dataItem, int sourceId, int destinationId) {
		this.dataItem = dataItem;
		this.sourceId = sourceId;
		this.destinationId = destinationId;
	}

	public DataItem getDataItem() {
		return dataItem;
	}

	public int getSourceId() {
		return sourceId;
	}

	public int getDestinationId() {
		return destinationId;
	}	
}
