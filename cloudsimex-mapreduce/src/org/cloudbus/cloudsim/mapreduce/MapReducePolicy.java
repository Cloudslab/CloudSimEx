package org.cloudbus.cloudsim.mapreduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Random;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This class implements the abstract policy for provisioning and scheduling of a DAG
 * in an IaaS data center. This method performs common tasks such as parsing the XML
 * file describing the DAG, printing the schedule, and returning provisioning and
 * scheduling decisions to the Workflow Engine. The abstract method must implement the
 * logic for filling the data structures related to provisioning and scheduling decisions. 
 *
 */
public abstract class MapReducePolicy extends DefaultHandler {
	
	protected int ownerId;
	private long availableExecTime;
	private long baseMIPS;
	protected VMOffers vmOffers;
	protected Random random;
	
	/*Data structures filled during XML parsing*/
	ArrayList<DataItem> originalDataItems;
	ArrayList<Task> entryTasks;
	ArrayList<Task> exitTasks;
	ArrayList<Task> tasks;
	
	/*Data structures to be filled by the concrete policy*/
	Hashtable<Integer,HashSet<Integer>> dataRequiredLocation;
	Hashtable<Integer,ArrayList<Task>> schedulingTable;
	ArrayList<ProvisionedVm> provisioningInfo;

	/**
	 * Fills the provisioning and scheduling data structures that are supplied
	 * to the Workflow Engine.
	 * @param availableExecTime time before the deadline
	 * @param vmOffers the VMOffers object that encapsulates information on available IaaS instances
	 */
	public abstract void doScheduling(long availableExecTime, VMOffers vmOffers);

	public MapReducePolicy(){

	}
	
	/**
	 * Reads the file specified as input, and processes the corresponding DAG, generating
	 * internal representation of provisioning and scheduling decision. WorkflowEngine
	 * queries for such an information to process the DAG.
	 *   
	 * @param dagFile Name of the DAG file.
	 */
	public void processDagFile(String dagFile, int ownerId, long availableExecTime, long baseMIPS, VMOffers vmOffers, long seed){
		this.ownerId = ownerId;
		this.availableExecTime = availableExecTime;
		this.baseMIPS = baseMIPS;
		this.vmOffers = vmOffers;
		this.random = new Random(seed);
		
		this.originalDataItems = new ArrayList<DataItem>();
		this.entryTasks = new ArrayList<Task>();
		this.exitTasks = new ArrayList<Task>();
		this.tasks = new ArrayList<Task>();
		
		this.dataRequiredLocation = new Hashtable<Integer,HashSet<Integer>>();
		this.schedulingTable = new Hashtable<Integer,ArrayList<Task>>();
		this.provisioningInfo = new ArrayList<ProvisionedVm>();
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			SAXParser sp = spf.newSAXParser();
			sp.parse(dagFile, this);		
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
		
	/**
	 * Determines the VMs where each dataItem will be required.
	 * 
	 * @return A hashtable containing each registered dataItem and the list of VMs ids where they are required. 
	 */
	public Hashtable<Integer,HashSet<Integer>> getDataRequiredLocation(){
		return dataRequiredLocation;
	}
	
	/**
	 * Determines the ordering of Tasks execution inside each VM.
	 * 
	 * @return A hashtable containing each VM and the list of Tasks, in execution order, in such VM. 
	 */
	public Hashtable<Integer,ArrayList<Task>> getScheduling(){
		return schedulingTable;
	}
		
	/**
	 * Returns the list of required VMs (number, characteristics, start and end times)
	 */
	public ArrayList<ProvisionedVm> getProvisioning(){
		return provisioningInfo;
	}
	
	public void printScheduling(long scheduleTime){
		System.out.println("-------------------------------------------");
		System.out.println("-- Schedule time (ms):"+scheduleTime);
		
		System.out.println("-- Provisioning:");
		for(ProvisionedVm vm:provisioningInfo){
			System.out.println("-- VM id:"+vm.getVm().getId()+" RAM:"+vm.getVm().getRam()+
					" start:"+vm.getStartTime()+" end:"+vm.getEndTime());
		}
		
		System.out.println("-- Scheduling:");
		for (ProvisionedVm vm:provisioningInfo){
			System.out.print("-- VM#"+vm.getVm().getId()+": ");
			for(Task t:schedulingTable.get(vm.getVm().getId())){
				System.out.print(t.getId()+" ");
			}
			System.out.println();
		}
		System.out.println();
		
		System.out.println("-- Data located at:");
		for (Entry<Integer, HashSet<Integer>> entry: dataRequiredLocation.entrySet()){
			System.out.print("-- Data id#"+entry.getKey()+": ");
			for(int loc:entry.getValue()){
				System.out.print(loc+" ");
			}
			System.out.println();
		}
		System.out.println();
		
		System.out.println("-------------------------------------------");
	}
			
	/********************************** SAX-related methods ****************************************/
	static Task currentTask;
	static int taskCont;
	static int dataItemCont;
	static Hashtable<String,Task> taskMap;
	static Hashtable<String,DataItem> dataItems;
	ArrayList<DataItem> generatedDataItems;
	
	public void startDocument(){
		currentTask=null;
		taskCont=0;
		dataItemCont=0;
		taskMap=new Hashtable<String,Task>();
		dataItems=new Hashtable<String,DataItem>();
		generatedDataItems=new ArrayList<DataItem>();
	}
	
	public void startElement(String uri, String localName, String qName, Attributes attributes){
		/*
		 * Elements can be one of: 'adag' 'job' 'uses' 'child' 'parent'
		 */
		if(qName.equalsIgnoreCase("adag")){//nothing to be done
			
		} else if(qName.equalsIgnoreCase("job")){//a new task is being declared
			String id = attributes.getValue("id");
			//String namespace = attributes.getValue("namespace");
			//String name = attributes.getValue("name");
			//String version = attributes.getValue("version");
			String runtime = attributes.getValue("runtime");
			
			double mean = Math.ceil(Double.parseDouble(runtime));
			double dbLength = mean*(1.0+random.nextGaussian()*0.1);
			dbLength*=baseMIPS;
			
			long length = (long) dbLength;
			
			Task task = new Task(new Cloudlet(taskCont,length,1,0,0,new UtilizationModelFull(),new UtilizationModelFull(),new UtilizationModelFull()),ownerId);
			taskMap.put(id, task);
			tasks.add(task);
			entryTasks.add(task);
			exitTasks.add(task);
			
			currentTask = task;
			taskCont++;
		} else if(qName.equalsIgnoreCase("uses")){//a file dependency from the current task
			String file = attributes.getValue("file");
			String link = attributes.getValue("link");
			//String register = attributes.getValue("register");
			//String transfer = attributes.getValue("transfer");
			//String optional = attributes.getValue("optional");
			//String type = attributes.getValue("type");
			String size = attributes.getValue("size");
			
			DataItem data;
			if (!dataItems.containsKey(file)){//file not declared yet; register
				//first, get the data size in kb
				long sizeInBytes = Long.parseLong(size);
				long sizeInKb = sizeInBytes/1024;
				data = new DataItem(dataItemCont,ownerId,file,sizeInKb);
				originalDataItems.add(data);
				dataItems.put(file, data);
				dataItemCont++;
			} else { //file already used by other task. Retrieve
				data = dataItems.get(file);
			}
			
			if(link.equalsIgnoreCase("input")){
				currentTask.addDataDependency(data);
			} else {
				currentTask.addOutput(data);
				generatedDataItems.add(data);
			}
		} else if(qName.equalsIgnoreCase("child")){//a task that depends on other(s)
			String ref = attributes.getValue("ref");
			currentTask = taskMap.get(ref);
			entryTasks.remove(currentTask);
		} else if(qName.equalsIgnoreCase("parent")){//a task that others depend on
			String ref = attributes.getValue("ref");
			Task parent = taskMap.get(ref);
			
			parent.addChild(currentTask);
			currentTask.addParent(parent);
			exitTasks.remove(parent);
		} else {
			Log.printLine("WARNING: Unknown XML element:"+qName);
		}
	}
		
	public void endDocument(){
		//parsing is completed. Cleanup auxiliary data structures and run the actual DAG provisioning/scheduling
		taskMap.clear();
		dataItems.clear();
		originalDataItems.removeAll(generatedDataItems);
		
		long startTime = System.currentTimeMillis();
		doScheduling(availableExecTime,vmOffers);
		long scheduleTime = System.currentTimeMillis()-startTime;
		printScheduling(scheduleTime);
		
		//make sure original dataItems are available on the required vms
		for(DataItem data:originalDataItems){
			if (!dataRequiredLocation.containsKey(data.getId())) dataRequiredLocation.put(data.getId(), new HashSet<Integer>());
			HashSet<Integer> requiredAt = dataRequiredLocation.get(data.getId());
				for(int at:requiredAt){
					data.addLocation(at);
				}
		}
	}
}
