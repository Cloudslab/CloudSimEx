package org.cloudbus.cloudsim.ex.mapreduce;

import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * This class extends data center to support simulation
 * of workflow applications including internal data
 * transfer delays if data has to be moved for task
 * running in other VM.
 */
public class OptimalMapReduceDatacenter extends Datacenter {
	
	public static final int UPDATE_NETWORK = 455671;
	public static final int TRANSFER_DATA_ITEM = 455672;
	public static final int DATA_ITEM_AVAILABLE = 455673;
	
	//billing interval in minutes
	public static final long BILLING_INTERVAL = 60; 
	
	private static final int MAX_VMS = 1000;
	
	//defines minimum quantum of time between events in seconds. It affects accuracy of the simulation
	protected static final double QUANTUM = 0.05;
	
	Hashtable<Integer,Vm> vmTable;
	Hashtable<Long,Channel> vmChannelTable;
	Hashtable<Vm,Long> vmCreationTime;
	Hashtable<Vm,Double> vmPrice;
	
	long basicCpuUnit;
	double bandwidth;
	double latency;
	double cohostedLatency;
	long averageCreationDelay;
	long budget; //output parameter: time of vms used during the simulation
	VMOffers vmOffers;
	
	public OptimalMapReduceDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy,
			double bandwidth, double latency, int basicCpuUnit, long averageCreationDelay, VMOffers vmOffers) throws Exception {
		super(name,characteristics,vmAllocationPolicy,null,0);
		
		this.vmTable = new Hashtable<Integer,Vm>();
		this.basicCpuUnit = basicCpuUnit;
		this.budget = 0;
		this.vmCreationTime = new Hashtable<Vm,Long>();
		this.vmPrice = new Hashtable<Vm,Double>();
				
		this.bandwidth = bandwidth;
		this.latency = latency;
		this.averageCreationDelay = averageCreationDelay;
		//if latency is smaller than minimum quantum of time, ignore it
		if (latency<QUANTUM) latency = 0.0;
		
		this.vmOffers = vmOffers;
		
		//latency for transmission within a host is 10% of the network latency
		this.cohostedLatency = latency/10;
		if(this.cohostedLatency<QUANTUM) cohostedLatency = 0.0;
		
		this.vmChannelTable = new Hashtable<Long,Channel>(); 
	}
		
	@Override
	protected void processOtherEvent(SimEvent ev) {		
		if (ev == null){
			Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Null event ignored.");
		} else {
			int tag = ev.getTag();
			switch(tag){
				case UPDATE_NETWORK: updateNetwork(); break;
				case TRANSFER_DATA_ITEM: processTransferDataItem(ev); break;
				case DATA_ITEM_AVAILABLE: processDataItemAvailable(ev); break;
				case CloudSimTags.END_OF_SIMULATION: shutdownEntity(); break;
				default: Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Unknown event ignored. Tag: "+tag);
			}
		}
	}
	
	@Override
	protected void processVmCreate(SimEvent ev, boolean ack) {
		super.processVmCreate(ev, false);//do not send ack to broker yet
		
		Vm vm = (Vm) ev.getData();
		Log.printLine(CloudSim.clock()+": VM #"+vm.getId()+" created.");
		vmCreationTime.put(vm, (long) CloudSim.clock());
		vmPrice.put(vm, getPrice(vm));
		vmTable.put(vm.getId(), vm);
		
		/*send the ack to ther broker with a random delay, to
		 * model delay in VM boot time (we are charged from the
		 * moment we request the Vm, but it takes a while for it
		 * to be usable)
		 */
		int[] data = new int[3];
		data[0] = getId();
		data[1] = vm.getId();
		data[2] = CloudSimTags.TRUE;

		send(vm.getUserId(),averageCreationDelay,CloudSimTags.VM_CREATE_ACK, data);
	}
	
	@Override
	protected void processVmDestroy(SimEvent ev, boolean ack) {
		//do last accounting for the vm
		Vm vm = (Vm) ev.getData();
		long startTime = vmCreationTime.remove(vm);
		Double price = vmPrice.remove(vm);
		long useInHours = updateVmUsage(startTime,price);
		Log.printLine(CloudSim.clock()+": VM #"+vm.getId()+" destroyed. Total usage time: "+useInHours+" h. Cost = $"+price+" cents/hour.");
		//vmTable.remove(vm.getId());
		
		super.processVmDestroy(ev, ack);
	}
						
	@Override	
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		updateCloudletProcessing();
		try {
			// gets the Cloudlet object
			Cloudlet cl = (Cloudlet) ev.getData();
			// checks whether this Cloudlet has finished or not
			if (cl.isFinished()){
				Log.printLine("WARNING: completed cloudlet resubmited. Submission ignored. Cloudlet id:"+cl.getCloudletId());
				return;
			}
		
			// process this Cloudlet to this CloudResource
			cl.setResourceParameter(getId(), 0.0, 0.0);

			int userId = cl.getUserId();
			int vmId = cl.getVmId();
			Host host = getVmAllocationPolicy().getHost(vmId, userId);
			Vm vm = host.getVm(vmId, userId);
			CloudletScheduler scheduler = vm.getCloudletScheduler();
			double estimatedFinishTime = scheduler.cloudletSubmit(cl);
			if (estimatedFinishTime<QUANTUM) estimatedFinishTime=QUANTUM;
			send(getId(),estimatedFinishTime,CloudSimTags.VM_DATACENTER_EVENT);
			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = cl.getCloudletId();
				data[2] = CloudSimTags.TRUE;

				// unique tag = operation tag
				sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
			}
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}

		checkCloudletCompletion();
	}
		
	@Override
	protected void updateCloudletProcessing() {
		if (CloudSim.clock() >= this.getLastProcessTime()+QUANTUM) {
			double smallerTime = Double.MAX_VALUE;
			for (Host host: getVmAllocationPolicy().getHostList()){
				double time = host.updateVmsProcessing(CloudSim.clock());
				if (time < smallerTime) smallerTime = time;
			}

			//if there are more events
			if (smallerTime < Double.MAX_VALUE) {
				double delay = smallerTime-CloudSim.clock();
				if (delay<QUANTUM) delay=QUANTUM;
				schedule(getId(), delay, CloudSimTags.VM_DATACENTER_EVENT);
			} 
			setLastProcessTime(CloudSim.clock());
		}
	}
	
	private void updateNetwork() {
		double smallerEvent = Double.POSITIVE_INFINITY;
			
		for(Channel channel: vmChannelTable.values()){
			//updates each channel
			if (CloudSim.clock() > channel.getLastUpdateTime()+QUANTUM) {
				double nextEvent = channel.updateTransmission(CloudSim.clock());
				if (nextEvent<smallerEvent) {
					smallerEvent = nextEvent;
				}
			}
			
			//process arrived dataItems, summing the latency
			LinkedList<Transmission> arrivingList= channel.getArrivedDataItems();
			for (Transmission tr: arrivingList){
				schedule(getId(),latency,DATA_ITEM_AVAILABLE,tr);
			}
		}
		
		if(smallerEvent!=Double.POSITIVE_INFINITY){
			if (smallerEvent-CloudSim.clock()<QUANTUM) smallerEvent = CloudSim.clock()+QUANTUM;
			schedule(getId(),smallerEvent,UPDATE_NETWORK);
		}
	}
	
	private void processTransferDataItem(SimEvent ev) {
		TransferDataEvent event = (TransferDataEvent) ev.getData();
		int sourceId = event.getSourceId();
		int destinationId = event.getDestinationId();
		DataItem data = event.getDataItem();
		
		//check if VMs are cohosted
		boolean cohosted=true;	
		if (vmTable.get(sourceId).getHost().getId()!=vmTable.get(destinationId).getHost().getId()){
			createChannel(sourceId,destinationId);
			cohosted=false;
		}
		
		addTransmission(data, destinationId, sourceId, cohosted);
	}
	
	private void processDataItemAvailable(SimEvent ev) {
		// send a message to the broker saying that the data item is available in the destination host
		Transmission tr = (Transmission) ev.getData();
		tr.getDataItem().addLocation(tr.getDestinationId());
		int owner = tr.getDataItem().getOwnerId();
		sendNow(owner,DATA_ITEM_AVAILABLE,tr);
	}

	private void addTransmission(DataItem data, int destinationId, int sourceId, boolean cohosted) {
		updateNetwork();
		
		Transmission transmission = new Transmission(data,sourceId,destinationId);
		if(cohosted){
			//vms are in the same host; Just add a small latency
			schedule(getId(),cohostedLatency,DATA_ITEM_AVAILABLE,transmission);
		} else {
			double delay = getChannel(sourceId, destinationId).addTransmission(transmission);
			if (delay>=QUANTUM){//schedules a completion event
				schedule(getId(),delay,UPDATE_NETWORK);
			} else {//very short transmission; remove from transmission
				getChannel(sourceId, destinationId).removeTransmission(transmission);
				//assume the data is already available
				schedule(getId(),latency,DATA_ITEM_AVAILABLE,transmission);
			}
		}
	}
	
	private void createChannel(int sourceId,int destinationId){
		long key = sourceId*MAX_VMS+destinationId;
		
		if(!vmChannelTable.containsKey(key)){
			Channel channel = new Channel(bandwidth);
			vmChannelTable.put(key, channel);
		}
	}
	
	private Channel getChannel(int sourceId,int destinationId){
		long key = sourceId*MAX_VMS+destinationId;
		
		return vmChannelTable.get(key);
	}
	
	private Double getPrice(Vm vm) {
		Hashtable<Vm,Double> vmOffersTable = vmOffers.getVmOffers();
		Double cost=0.0;
		
		for (Entry<Vm, Double> entry: vmOffersTable.entrySet()){
			Vm v = entry.getKey();
			//use memory due to precision of equality
			if (v.getRam()==vm.getRam()){
				//vm type found
				cost = entry.getValue();
			}
		}
	
		return cost;
	}
		
	//Accounts utilization of VMs inside the data center
	private long updateVmUsage(long startTime, Double price) {
		long currentTime = (long) Math.ceil(CloudSim.clock());
		long runTimeInSeconds = currentTime-startTime;
		
		/* total runtime of the vm in seconds. This is too precise for
		 * simulation purposes. Therefore, we reduce it to minutes discarding the
		 * remainders. Thus, 64 seconds counts as 1 minute. Considering that
		 * the final precision is one hour, this does not induce significant inaccuracy.
		*/
		long runTimeInMinutes = runTimeInSeconds/60;
		
		//Upfront charge by fixed time slot
		long runTimeInHours = runTimeInMinutes/60;
		if (runTimeInMinutes%60!=0) runTimeInHours++;
				
		this.budget += runTimeInHours*price;
		
		return runTimeInHours;
	}
		
	//Simulation output
	public void printSummary(){
		DecimalFormat df = new DecimalFormat("#.##");
		double cost = ((double)this.budget)/100.00;
		
		Log.printLine();
		Log.printLine("======== DATACENTER SUMMARY ========");
		Log.printLine("= Cost: "+df.format(cost));
		Log.printLine("========== END OF SUMMARY =========");
	}
}
