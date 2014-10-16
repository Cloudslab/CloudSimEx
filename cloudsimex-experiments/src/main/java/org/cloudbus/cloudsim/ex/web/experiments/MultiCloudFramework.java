package org.cloudbus.cloudsim.ex.web.experiments;

import static org.cloudbus.cloudsim.Consts.DAY;
import static org.cloudbus.cloudsim.Consts.HOUR;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.HOURS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.IAutoscalingPolicy;
import org.cloudbus.cloudsim.ex.billing.EC2OnDemandPolicy;
import org.cloudbus.cloudsim.ex.billing.ExamplePrices;
import org.cloudbus.cloudsim.ex.billing.GoogleOnDemandPolicy;
import org.cloudbus.cloudsim.ex.billing.IVmBillingPolicy;
import org.cloudbus.cloudsim.ex.delay.ExampleGaussianDelaysPerType;
import org.cloudbus.cloudsim.ex.delay.GaussianByTypeBootDelay;
import org.cloudbus.cloudsim.ex.delay.IVMBootDelayDistribution;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.ex.disk.HddDataCenter;
import org.cloudbus.cloudsim.ex.disk.HddHost;
import org.cloudbus.cloudsim.ex.disk.HddPe;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.disk.VmDiskScheduler;
import org.cloudbus.cloudsim.ex.geolocation.IPMetadata;
import org.cloudbus.cloudsim.ex.geolocation.geoip2.GeoIP2IPGenerator;
import org.cloudbus.cloudsim.ex.geolocation.geoip2.GeoIP2PingERService;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.TextUtil;
import org.cloudbus.cloudsim.ex.vm.VMMetadata;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.cloudbus.cloudsim.ex.web.BaseWebLoadBalancer;
import org.cloudbus.cloudsim.ex.web.CompositeGenerator;
import org.cloudbus.cloudsim.ex.web.CompressLoadBalancer;
import org.cloudbus.cloudsim.ex.web.IGenerator;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.RandomListGenerator;
import org.cloudbus.cloudsim.ex.web.RoundRobinDBBalancer;
import org.cloudbus.cloudsim.ex.web.StatGenerator;
import org.cloudbus.cloudsim.ex.web.WebCloudlet;
import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.IWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.RandomIPWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.StatWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.brokers.CompressedAutoscalingPolicy;
import org.cloudbus.cloudsim.ex.web.workload.brokers.EntryPoint;
import org.cloudbus.cloudsim.ex.web.workload.brokers.IEntryPoint;
import org.cloudbus.cloudsim.ex.web.workload.brokers.RoundRobinWebLoadBalancer;
import org.cloudbus.cloudsim.ex.web.workload.brokers.Route53EntryPoint;
import org.cloudbus.cloudsim.ex.web.workload.brokers.SimpleAutoScalingPolicy;
import org.cloudbus.cloudsim.ex.web.workload.brokers.WebBroker;
import org.cloudbus.cloudsim.ex.web.workload.freq.CompositeValuedSet;
import org.cloudbus.cloudsim.ex.web.workload.freq.FrequencyFunction;
import org.cloudbus.cloudsim.ex.web.workload.freq.PeriodicStochasticFrequencyFunction;
import org.cloudbus.cloudsim.ex.web.workload.sessions.GeneratorsUtil;
import org.cloudbus.cloudsim.ex.web.workload.sessions.StatSessionGenerator;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.uncommons.maths.random.SecureRandomSeedGenerator;
import org.uncommons.maths.random.SeedGenerator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * Prices - http://aws.amazon.com/pricing/
 * 
 * @author nikolay.grozev
 * 
 */
public class MultiCloudFramework {

    private static SeedGenerator SEED_GEN = new SecureRandomSeedGenerator();

    private static final Set<String> EURO_CODES = Collections.unmodifiableSet(new LinkedHashSet<>(
	    Arrays.asList("BE", "FR", "AT", "BG", "IT", "PL", "CZ", "CY", "PT", "DK", "LV", "RO", "DE",
		    "LT", "SI", "EE", "LU", "SK", "IE", "HU", "FI", "EL", "MT", "SE", "ES", "NL", "UK")));

    private static final Set<String> US_CODES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList("US")));

    private GeoIP2IPGenerator euroIPGen;
    private GeoIP2IPGenerator usIPGen;
    private GeoIP2PingERService geoService;

    private static final String DUBLIN_IP = "5.149.168.0";
    private static final String NEW_YORK_IP = "74.221.217.130";

    private static final String HAMINA_FINLAND_IP = "80.222.136.0";
    private static final String DALAS_IP = "74.125.126.0";

    private static final int MIPS_VM_GOOGLE = (int) (10000 * (2600.0 / 3400.0));
    private static final int MIPS_DB_VM_EC2 = (int) (10000 * ((2266 * (100 - 0.241971) / 100) / 3400.0));
    private static final int MIPS_AS_VM_EC2 = (int) (10000 * ((2666 * (100 - 3.028893) / 100) / 3400.0));

    public static String DEF_DIR = "multi-cloud/";
    public static String RESULT_DIR = DEF_DIR + "stat/";

    protected int simulationLength = (DAY + HOUR / 2);
    protected int step = 60;

    protected int n = 5;
    protected int latencySLA = 30;
    protected double wldFactor = 10;
    protected double monitoringPeriod = 0.01;
    protected double autoscalingPeriod = 10;

    // AutoScaling
    protected double autoscaleTriggerCPU = 0.70;
    protected double autoscaleTriggerRAM = 0.70;

    // Load balancing
    protected double loadbalancingThresholdCPU = 0.70;
    protected double loadbalancingThresholdRAM = 0.70;

    // EC2 autoscaling
    private final double coolDownFactor = 150;
    private final double scaleDownFactor = 0.1;
    private final double scaleUpFactor = 0.8;

    // Num of VMs in the DB layer 
    private int numDBs = 15;

    private boolean baseline = false;

    protected String experimentName = "Multi-Cloud Framework Experiment";
    public String resultDIR = RESULT_DIR;

    private static final DataItem DATA_EURO1 = new DataItem(5);
    private static final DataItem DATA_EURO2 = new DataItem(5);
    private static final DataItem DATA_EURO3 = new DataItem(5);
    private static final DataItem DATA_EURO4 = new DataItem(5);
    private static final DataItem[] EURO_DATA_ITEMS = new DataItem[] { DATA_EURO1, DATA_EURO2, DATA_EURO3, DATA_EURO4 };

    private static final DataItem DATA_US1 = new DataItem(5);
    private static final DataItem DATA_US2 = new DataItem(5);
    private static final DataItem DATA_US3 = new DataItem(5);
    private static final DataItem DATA_US4 = new DataItem(5);
    private static final DataItem[] US_DATA_ITEMS = new DataItem[] { DATA_US1, DATA_US2, DATA_US3, DATA_US4 };

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	// ExperimentsUtil.parseExperimentParameters(args);
	if (args.length == 0) {
	    MultiCloudFramework experiment = new MultiCloudFramework();
	    Properties props = new Properties();
	    try (InputStream is = Files.newInputStream(Paths.get("../custom_log.properties"))) {
		props.load(is);
	    }

	    props.put(CustomLog.FILE_PATH_PROP_KEY,
		    experiment.resultDIR + String.format("%s.log", MultiCloudFramework.class.getSimpleName()));
	    CustomLog.configLogger(props);
	    experiment.runExperimemt();

	} else {
	    configandRun(args);
	}
    }

    public static void configandRun(final String[] args) throws IOException {
	MultiCloudFramework experiment = new MultiCloudFramework();
	int i = 1;
	experiment.n = Integer.parseInt(args[i++]);
	experiment.latencySLA = Integer.parseInt(args[i++]);
	experiment.wldFactor = Double.parseDouble(args[i++]);
	experiment.monitoringPeriod = Double.parseDouble(args[i++]);
	experiment.autoscalingPeriod = Double.parseDouble(args[i++]);

	// AutoScaling
	experiment.autoscaleTriggerCPU = Double.parseDouble(args[i++]);
	experiment.autoscaleTriggerRAM = Double.parseDouble(args[i++]);

	// Load balancing
	experiment.loadbalancingThresholdCPU = Double.parseDouble(args[i++]);
	experiment.loadbalancingThresholdRAM = Double.parseDouble(args[i++]);

	// VMs in DB layer
	experiment.numDBs = Integer.parseInt(args[i++]);
	
	experiment.baseline = Boolean.parseBoolean(args[i++]);

	experiment.experimentName = String.format("[%s]Exp-wldf(%d)-sla(%d)-n(%d)-db(%d)",
		experiment.baseline? "Baseline" : "Run",
		(int) experiment.wldFactor, experiment.latencySLA, experiment.n, experiment.numDBs);
	experiment.resultDIR = String.format("%swldf(%s)-wldf-%d-sla-%d-n-%d-db-%d/", RESULT_DIR,
		experiment.baseline ? "baseline" : "run",
		(int) experiment.wldFactor, experiment.latencySLA, experiment.n, experiment.numDBs);
	File resultDirFile = new File(experiment.resultDIR);
	if (!resultDirFile.exists()) {
	    resultDirFile.mkdir();
	}

	Properties props = new Properties();
	try (InputStream is = Files.newInputStream(Paths.get(args[0]))) {
	    props.load(is);
	}

	props.put(CustomLog.FILE_PATH_PROP_KEY,
		experiment.resultDIR + String.format("%s.log", MultiCloudFramework.class.getSimpleName()));
	CustomLog.configLogger(props);
	experiment.runExperimemt();
    }

    /**
     * Sets up the experiment and runs it.
     * 
     * @throws SecurityException
     * @throws IOException
     */
    public final void runExperimemt() throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	CustomLog.print("Simulation summary:");
	CustomLog.printf("latencySLA=%.2f", (double) latencySLA);
	CustomLog.printf("n=%d", n);
	CustomLog.printf("wldFactor=%.2f", wldFactor);
	CustomLog.printf("monitoringPeriod=%.2f", monitoringPeriod);
	CustomLog.printf("autoscalingPeriod=%.2f", autoscalingPeriod);
	CustomLog.printf("autoscaleTriggerCPU=%.2f", autoscaleTriggerCPU);
	CustomLog.printf("autoscaleTriggerRAM=%.2f", autoscaleTriggerRAM);
	CustomLog.printf("loadbalancingThresholdCPU=%.2f", loadbalancingThresholdCPU);
	CustomLog.printf("loadbalancingThresholdRAM=%.2f", loadbalancingThresholdRAM);
	CustomLog.printf("numDBs=%d", numDBs);
	CustomLog.printf("Baseline=%s", Boolean.toString(baseline));
	CustomLog.printLine("");
	CustomLog.print("Workload frequencies:");
	for (String period : getPeriods(wldFactor)) {
	    CustomLog.printLine("\t" + period);

	}
	CustomLog.printLine("");

	CustomLog.print("Step 0: Initialising IP services....");
	euroIPGen = new GeoIP2IPGenerator(EURO_CODES);
	usIPGen = new GeoIP2IPGenerator(US_CODES);
	geoService = new GeoIP2PingERService();

	try {
	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step1: Initialize the CloudSim package.
	    CustomLog.print("Step 1: Initialising CloudSIm....");

	    int numBrokers = 4; // number of brokers to use
	    boolean trace_flag = false;
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag, 0.001);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 2: Create Datacenters
	    CustomLog.print("Step 2: Creating Data Centres....");

	    int hardwareMips = (int) (10000 * 3.4);
	    int hardwareMiops = 7500;
	    int hardwareRam = 2048 * 4;
	    Datacenter dcEuroGoogle = createDC("Euro-Google", EURO_DATA_ITEMS, hardwareMips,
		    hardwareMiops, hardwareRam,
		    new GaussianByTypeBootDelay(ExampleGaussianDelaysPerType.EC2_BOOT_TIMES, SEED_GEN));
	    Datacenter dcEuroEC2 = createDC("Euro-EC2", EURO_DATA_ITEMS, hardwareMips,
		    hardwareMiops, hardwareRam,
		    new GaussianByTypeBootDelay(ExampleGaussianDelaysPerType.EC2_BOOT_TIMES, SEED_GEN));

	    Datacenter dcUSGoogle = createDC("US-Google", US_DATA_ITEMS, hardwareMips, hardwareMiops, hardwareRam,
		    new GaussianByTypeBootDelay(ExampleGaussianDelaysPerType.EC2_BOOT_TIMES, SEED_GEN));
	    Datacenter dcUSEC2 = createDC("US-EC2", US_DATA_ITEMS, hardwareMips, hardwareMiops, hardwareRam,
		    new GaussianByTypeBootDelay(ExampleGaussianDelaysPerType.EC2_BOOT_TIMES, SEED_GEN));

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 3: Create Brokers and set the appropriate billing policies
	    CustomLog.print("Step 3: Creating Brokers and billing policies....");

	    IVmBillingPolicy googleEUBilling = new GoogleOnDemandPolicy(ExamplePrices.GOOGLE_NIX_OS_PRICES_EUROPE);
	    WebBroker brokerEuroGoogle = new FlushWebBroker("Euro-Google", step, simulationLength, monitoringPeriod,
		    autoscalingPeriod, dcEuroGoogle.getId(), "EU");
	    brokerEuroGoogle.addAutoScalingPolicy(createAutoscalingPolicy());
	    brokerEuroGoogle.setVMBillingPolicy(googleEUBilling);

	    IVmBillingPolicy ec2EUBilling = new EC2OnDemandPolicy(ExamplePrices.EC2_NIX_OS_PRICES_IRELAND);
	    WebBroker brokerEuroEC2 = new FlushWebBroker("Euro-EC2", step, simulationLength, monitoringPeriod,
		    autoscalingPeriod, dcEuroEC2.getId(), "EU");
	    brokerEuroEC2.addAutoScalingPolicy(createAutoscalingPolicy());
	    brokerEuroEC2.setVMBillingPolicy(ec2EUBilling);

	    IVmBillingPolicy googleUSBilling = new GoogleOnDemandPolicy(ExamplePrices.GOOGLE_NIX_OS_PRICES_US);
	    WebBroker brokerUSGoogle = new FlushWebBroker("US-Google", step, simulationLength, monitoringPeriod,
		    autoscalingPeriod, dcUSGoogle.getId(), "US");
	    brokerUSGoogle.addAutoScalingPolicy(createAutoscalingPolicy());
	    brokerUSGoogle.setVMBillingPolicy(googleUSBilling);

	    IVmBillingPolicy ec2USBilling = new EC2OnDemandPolicy(ExamplePrices.EC2_NIX_OS_PRICES_VIRGINIA);
	    WebBroker brokerUSEC2 = new FlushWebBroker("US-EC2", step, simulationLength, monitoringPeriod,
		    autoscalingPeriod, dcUSEC2.getId(), "US");
	    brokerUSEC2.addAutoScalingPolicy(createAutoscalingPolicy());
	    brokerUSEC2.setVMBillingPolicy(ec2USBilling);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 4: Set up the entry point
	    CustomLog.print("Step 4: Setting up entry points....");

	    IEntryPoint entryPoint = baseline ? new Route53EntryPoint(geoService, 1) : new EntryPoint(geoService, 1,
		    latencySLA);
	    for (WebBroker broker : new WebBroker[] { brokerEuroGoogle, brokerEuroEC2, brokerUSGoogle, brokerUSEC2 }) {
		broker.addEntryPoint(entryPoint);
	    }

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 5: Create virtual machines
	    CustomLog.print("Step 5: Setting up VMs....");
	    	    
	    int numApp = n < 1 ? 1 : n;
	    VMMetadata ec2Meta = new VMMetadata();
	    ec2Meta.setOS(Consts.NIX_OS);
	    ec2Meta.setType("m1.small");
	    VMMetadata googleMeta = new VMMetadata();
	    googleMeta.setOS(Consts.NIX_OS);
	    googleMeta.setType("n1-standard-1-d");

	    List<HddVm> appServersEuroGoogle =
		    createVMs("AS-Google-Euro", brokerEuroGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numApp,
			    googleMeta);
	    List<HddVm> dbServersEuroGoogle =
		    createVMs("DB-Google-Euro", brokerEuroGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numDBs,
			    googleMeta);

	    List<HddVm> appServersEuroEC2 =
		    createVMs("AS-EC2-Euro", brokerEuroEC2.getId(), MIPS_AS_VM_EC2, 7500, 1666, numApp, ec2Meta);
	    List<HddVm> dbServersEuroEC2 =
		    createVMs("DB-EC2-Euro", brokerEuroEC2.getId(), MIPS_DB_VM_EC2, 7500, 1666, numDBs, ec2Meta);

	    List<HddVm> appServersUSGoogle =
		    createVMs("AS-Google-US", brokerUSGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numApp, googleMeta);
	    List<HddVm> dbServersUSGoogle =
		    createVMs("DB-Google-US", brokerUSGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numDBs, googleMeta);

	    List<HddVm> appServersUSEC2 =
		    createVMs("AS-EC2-US", brokerUSEC2.getId(), MIPS_AS_VM_EC2, 7500, 1666, numApp, ec2Meta);
	    List<HddVm> dbServersUSEC2 =
		    createVMs("DB-EC2-US", brokerUSEC2.getId(), MIPS_DB_VM_EC2, 7500, 1656, numDBs, ec2Meta);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 6: Create load balancers
	    CustomLog.print("Step 6: Setting up load balancers....");

	    ILoadBalancer balancerEuroGoogle =
		    createLoadBalancer(brokerEuroGoogle, appServersEuroGoogle, dbServersEuroGoogle, HAMINA_FINLAND_IP);
	    brokerEuroGoogle.addLoadBalancer(balancerEuroGoogle);

	    ILoadBalancer balancerEuroEC2 =
		    createLoadBalancer(brokerEuroEC2, appServersEuroEC2, dbServersEuroEC2, DUBLIN_IP);
	    brokerEuroEC2.addLoadBalancer(balancerEuroEC2);

	    ILoadBalancer balancerUSGoogle =
		    createLoadBalancer(brokerUSGoogle, appServersUSGoogle, dbServersUSGoogle, DALAS_IP);
	    brokerUSGoogle.addLoadBalancer(balancerUSGoogle);

	    ILoadBalancer balancerUSEC2 =
		    createLoadBalancer(brokerUSEC2, appServersUSEC2, dbServersUSEC2, NEW_YORK_IP);
	    brokerUSEC2.addLoadBalancer(balancerUSEC2);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 7: Add the virtual machines for the data centers
	    CustomLog.print("Step 7: Add VMs to the data centres....");

	    ImmutableMap<ILoadBalancer, WebBroker> balancersToBrokers = ImmutableMap
		    .<ILoadBalancer, WebBroker> builder()
		    .put(balancerEuroGoogle, brokerEuroGoogle)
		    .put(balancerEuroEC2, brokerEuroEC2)
		    .put(balancerUSGoogle, brokerUSGoogle)
		    .put(balancerUSEC2, brokerUSEC2).build();

	    for (Map.Entry<ILoadBalancer, WebBroker> el : balancersToBrokers.entrySet()) {
		List<Vm> vmlistDC1 = new ArrayList<Vm>();
		vmlistDC1.addAll(el.getKey().getAppServers());
		vmlistDC1.addAll(el.getKey().getDbBalancer().getVMs());

		el.getValue().submitVmList(vmlistDC1);
	    }

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 8: Define the workload and associate it with load balancers
	    CustomLog.print("Step 8: Define workload....");

	    IWorkloadGenerator workloadEuroGoogle = generateWorkload(
		    brokerEuroGoogle.getId(),
		    0 * HOUR,
		    ImmutableMap.<String[], Double> of(new String[] { "US" }, 1.0, new String[] { "EU" }, 10.0),
		    euroIPGen, wldFactor);
	    brokerEuroGoogle.addWorkloadGenerators(Arrays.asList(workloadEuroGoogle), balancerEuroGoogle.getAppId());
	    IWorkloadGenerator workloadEuroEC2 = generateWorkload(
		    brokerEuroEC2.getId(),
		    0 * HOUR,
		    ImmutableMap.<String[], Double> of(new String[] { "US" }, 1.0, new String[] { "EU" }, 10.0),
		    euroIPGen, wldFactor);
	    brokerEuroEC2.addWorkloadGenerators(Arrays.asList(workloadEuroEC2), balancerEuroGoogle.getAppId());

	    IWorkloadGenerator workloadUSGoogle = generateWorkload(
		    brokerUSGoogle.getId(),
		    12 * HOUR,
		    ImmutableMap.<String[], Double> of(new String[] { "US" }, 10.0, new String[] { "EU" }, 1.0),
		    usIPGen, wldFactor);
	    brokerUSGoogle.addWorkloadGenerators(Arrays.asList(workloadUSGoogle), balancerEuroEC2.getAppId());
	    IWorkloadGenerator workloadUSEC2 = generateWorkload(
		    brokerUSEC2.getId(),
		    12 * HOUR,
		    ImmutableMap.<String[], Double> of(new String[] { "US" }, 10.0, new String[] { "EU" }, 1.0),
		    usIPGen, wldFactor);
	    brokerUSEC2.addWorkloadGenerators(Arrays.asList(workloadUSEC2), balancerEuroEC2.getAppId());

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 9: Starts the simulation
	    CustomLog.print("Step 9: Start simulation....");
	    CustomLog.printLine("");

	    CloudSim.startSimulation();

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();

	    // Print VMs
	    String[] vmProperties = new String[] { "Id", "Name", "Status", "SubmissionTime", "StartTime", "EndTime",
		    "LifeDuration" };

	    LinkedHashMap<String, Function<? extends Vm, String>> virtualProps = new LinkedHashMap<>();
	    virtualProps.put("Cost", new BillVmFunction(brokerEuroGoogle.getVMBillingPolicy()));
	    virtualProps.put("r_submitTime", F_READABLE_SUBMISSION_TIME);
	    virtualProps.put("r_startTime", F_READABLE_START_TIME);
	    virtualProps.put("r_endTime", F_READABLE_END_TIME);

	    CustomLog.redirectToFile(resultDIR + "VM-EuroGoogle.csv");
	    CustomLog.printResults(HddVm.class, vmProperties, virtualProps, brokerEuroGoogle.getVmList());

	    virtualProps.put("Cost", new BillVmFunction(brokerEuroEC2.getVMBillingPolicy()));
	    CustomLog.redirectToFile(resultDIR + "VM-EuroEC2.csv");
	    CustomLog.printResults(HddVm.class, vmProperties, virtualProps, brokerEuroEC2.getVmList());

	    virtualProps.put("Cost", new BillVmFunction(brokerUSGoogle.getVMBillingPolicy()));
	    CustomLog.redirectToFile(resultDIR + "VM-USGoogle.csv");
	    CustomLog.printResults(HddVm.class, vmProperties, virtualProps, brokerUSGoogle.getVmList());

	    virtualProps.put("Cost", new BillVmFunction(brokerUSEC2.getVMBillingPolicy()));
	    CustomLog.redirectToFile(resultDIR + "VM-USEC2.csv");
	    CustomLog.printResults(HddVm.class, vmProperties, virtualProps, brokerUSEC2.getVmList());
	    CustomLog.print(brokerUSEC2.bill());

	    // Print sessions
	    LinkedHashMap<String, Function<? extends WebSession, String>> sessVirtualProps = new LinkedHashMap<>();
	    sessVirtualProps.put("LatDelay", new DelayLatencyFunction(geoService));
	    sessVirtualProps.put("Meta", F_SESSION_META);
	    sessVirtualProps.put("Latency", new LatencyFunction(geoService));
	    sessVirtualProps.put("UserLocation", new SourceAddressFunction(geoService));
	    sessVirtualProps.put("UserLat", new SourceCoordFunction(geoService, true));
	    sessVirtualProps.put("UserLon", new SourceCoordFunction(geoService, false));
	    sessVirtualProps.put("DCLocation", new DCAddressFunction(geoService));
	    sessVirtualProps.put("DCLat", new DCCoordFunction(geoService, true));
	    sessVirtualProps.put("DCLong", new DCCoordFunction(geoService, false));
	    sessVirtualProps.put("URL", new URLFunction(geoService));

	    CustomLog.redirectToFile(resultDIR + "Sessions-BrokerEuroGoogle.csv");
	    CustomLog.printResults(WebSession.class, sessVirtualProps, brokerEuroGoogle.getServedSessions());

	    CustomLog.redirectToFile(resultDIR + "Sessions-BrokerEuroEC2.csv");
	    CustomLog.printResults(WebSession.class, sessVirtualProps, brokerEuroEC2.getServedSessions());

	    CustomLog.redirectToFile(resultDIR + "Sessions-BrokerUSGoogle.csv");
	    CustomLog.printResults(WebSession.class, sessVirtualProps, brokerUSGoogle.getServedSessions());

	    CustomLog.redirectToFile(resultDIR + "Sessions-BrokerUSEC2.csv");
	    CustomLog.printResults(WebSession.class, sessVirtualProps, brokerUSEC2.getServedSessions());

	    // Additional billing:
	    CustomLog.redirectToFile(resultDIR + "AdditionalBill.csv");
	    if (baseline) {
		// http://aws.amazon.com/pricing/
		int numVM = brokerEuroGoogle.getVmList().size() + brokerEuroEC2.getVmList().size()
			+ brokerUSGoogle.getVmList().size() + brokerUSEC2.getVmList().size();
		CustomLog.printf("Route 53: %d hits: $%.4f", entryPoint.getSessionsDispatched(),
			((double) entryPoint.getSessionsDispatched() / Consts.MILLION) * 0.75);
		CustomLog.printf("4 Elastic load balancer: $%.4f", 4 * 24 * 0.025);
		CustomLog.printf("Amazon CloudWatch: $%.4f", 4 * (0.1 + 0.5 + 3.50 * numVM) / 30.0);
	    } else {
		BigDecimal euroGooglePrice = brokerEuroGoogle.getVMBillingPolicy().bill(
			Arrays.asList(brokerEuroGoogle.getVmList().get(0)), DAY);
		BigDecimal euroEC2Price = brokerEuroEC2.getVMBillingPolicy().bill(
			Arrays.asList(brokerEuroEC2.getVmList().get(0)), DAY);
		BigDecimal usGooglePrice = brokerUSGoogle.getVMBillingPolicy().bill(
			Arrays.asList(brokerUSGoogle.getVmList().get(0)), DAY);
		BigDecimal usEc2Price = brokerUSEC2.getVMBillingPolicy().bill(
			Arrays.asList(brokerUSEC2.getVmList().get(0)), DAY);

		double sum = euroGooglePrice.doubleValue() +
			euroEC2Price.doubleValue() +
			usGooglePrice.doubleValue() +
			usEc2Price.doubleValue();

		CustomLog.printf("DC Controller: $%.2f, $%.2f, $%.2f, $%.2f; Cost=$%.2f",
			euroGooglePrice.doubleValue(), euroEC2Price.doubleValue(),
			usGooglePrice.doubleValue(), usEc2Price.doubleValue(), sum);
		CustomLog.printf("Load Balancer: $%.2f, $%.2f, $%.2f, $%.2f; Cost=$%.2f",
			euroGooglePrice.doubleValue(), euroEC2Price.doubleValue(),
			usGooglePrice.doubleValue(), usEc2Price.doubleValue(), sum);
		CustomLog.printf("Admission Controller: $%.2f, $%.2f, $%.2f, $%.2f; Cost=$%.2f",
			euroGooglePrice.doubleValue(), euroEC2Price.doubleValue(), usGooglePrice.doubleValue(),
			usEc2Price.doubleValue(), sum);
		CustomLog.printf("Entry Point: $%.2f, $%.2f, $%.2f, $%.2f; Cost=$%.2f", euroGooglePrice.doubleValue(),
			euroEC2Price.doubleValue(), usGooglePrice.doubleValue(), usEc2Price.doubleValue(), sum);
	    }

	    for(WebBroker b : new WebBroker[]{brokerEuroGoogle, brokerEuroEC2, brokerUSGoogle, brokerUSEC2}) {
		CustomLog.printf("%-25s AS: $%.2f", 
			b.toString(),
			b.getVMBillingPolicy().bill(b.getLoadBalancers().get(1l).getAppServers(), DAY).doubleValue());
		CustomLog.printf("%-25s DB: $%.2f", 
			b.toString(),
			b.getVMBillingPolicy().bill(b.getLoadBalancers().get(1l).getDbBalancer().getVMs(), DAY).doubleValue());
	    }
	    
	    
	    CustomLog.flush();

	    System.err.println(experimentName + ": Archiving the results into: "
		    + createZipFileName(resultDIR, RESULT_DIR));
	    archiveDir(resultDIR, RESULT_DIR);

	    System.err.println();
	    System.err.println(experimentName + ": Simulation is finished!");
	} catch (Exception e) {
	    System.err.println(experimentName + ": The simulation has been terminated due to an unexpected error");
	    e.printStackTrace();
	}
	System.err.println(experimentName + ": Finished in " +
		(System.currentTimeMillis() - simulationStart) / 1000 + " seconds");
    }

    public IAutoscalingPolicy createAutoscalingPolicy() {
	return baseline ? new SimpleAutoScalingPolicy(1, scaleUpFactor, scaleDownFactor, coolDownFactor)
		: new CompressedAutoscalingPolicy(1, autoscaleTriggerCPU,
			autoscaleTriggerRAM, n, autoscalingPeriod);
    }

    public BaseWebLoadBalancer createLoadBalancer(WebBroker broker, List<HddVm> appServers, List<HddVm> dbServers,
	    String ip) {
	return baseline ?
		new RoundRobinWebLoadBalancer(1, ip, appServers,
			new RoundRobinDBBalancer(dbServers),
			broker) :
		new CompressLoadBalancer(broker,
			1, ip, appServers, new RoundRobinDBBalancer(dbServers),
			loadbalancingThresholdCPU, loadbalancingThresholdRAM);
    }

    protected IWorkloadGenerator generateWorkload(final int userId, final double nullPoint,
	    final Map<String[], Double> valuesAndFreqs, GeoIP2IPGenerator ipGen, double f) {
	String[] periods = getPeriods(f);

	Map<String, List<Double>> asDefs;
	Map<String, List<Double>> dbDefs;
	try (InputStream asIO = new FileInputStream(DEF_DIR + "web_cloudlets.txt");
		InputStream dbIO = new FileInputStream(DEF_DIR + "db_cloudlets.txt")) {
	    asDefs = GeneratorsUtil.parseStream(asIO);
	    dbDefs = GeneratorsUtil.parseStream(dbIO);
	    StatSessionGenerator sessGen = new SynchDataStatGenerator(GeneratorsUtil.cloneDefs(asDefs),
		    GeneratorsUtil.cloneDefs(dbDefs), userId, step, new RandomListGenerator<>(valuesAndFreqs));

	    double unit = HOUR;
	    double periodLength = DAY;

	    FrequencyFunction freqFun = new PeriodicStochasticFrequencyFunction(unit, periodLength, nullPoint,
		    CompositeValuedSet.createCompositeValuedSet(SEED_GEN, periods));

	    return new RandomIPWorkloadGenerator(new StatWorkloadGenerator(SEED_GEN, freqFun, sessGen),
		    ipGen, geoService);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private String[] getPeriods(double f) {
	String[] periods = new String[] {
		String.format("[%d,%d] m=%f  std=%.2f", HOURS[0], HOURS[6], 10d * f, 1d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[6], HOURS[7], 30d * f, 2d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[7], HOURS[10], 50d * f, 3d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[10], HOURS[14], 100d * f, 4d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[14], HOURS[17], 50d * f, 3d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[17], HOURS[18], 30d * f, 2d),
		String.format("(%d,%d] m=%.2f  std=%.2f", HOURS[18], HOURS[24], 10d * f, 1d) };
	return periods;
    }

    private List<HddVm> createVMs(String name, final int brokerId, final int mips, final int ioMips, final int ram,
	    int numberOfVms,
	    VMMetadata meta) {
	List<HddVm> hddVMs = new ArrayList<>();
	for (int i = 0; i < numberOfVms; i++) {
	    // VM description
	    // int mips = 10000;
	    // int ioMips = 10000;
	    long size = 10000; // image size (MB)
	    // int ram = 512; // vm memory (MB)
	    long bw = 1000;
	    int pesNumber = 1; // number of cpus
	    String vmm = "Xen"; // VMM name

	    HddVm hddVm = new HddVm(name, brokerId, mips, ioMips, pesNumber,
		    ram, bw, size, vmm, new HddCloudletSchedulerTimeShared(), meta.clone(), new Integer[0]);
	    hddVMs.add(hddVm);
	}
	return hddVMs;
    }

    /*
     * https://developers.google.com/compute/docs/instances
     */
    // mips = (int) (10000 * 3.4), miops = 7500, ram = 2048*4
    private Datacenter createDC(final String name, final DataItem[] dataItems, final int mips, final int miops,
	    final int ramInMb, IVMBootDelayDistribution delay) {
	List<Host> hostList = new ArrayList<Host>();

	for (int i = 0; i < 100; i++) {
	    HddHost host = createHost(dataItems, mips, miops, ramInMb);
	    hostList.add(host);
	}

	Datacenter datacenter = createDCWithHosts(name, hostList, delay);

	return datacenter;
    }

    private static Datacenter createDCWithHosts(final String name, final List<Host> hostList,
	    IVMBootDelayDistribution delay) {
	String arch = "x86"; // system architecture
	String os = "Linux"; // operating system
	String vmm = "Xen";
	double time_zone = 10.0; // time zone this resource located
	double cost = 3.0; // the cost of using processing in this resource
	double costPerMem = 0.05; // the cost of using memory in this resource
	double costPerStorage = 0.001; // the cost of using storage in this
	// resource
	double costPerBw = 0.0; // the cost of using bw in this resource
	LinkedList<Storage> storageList = new LinkedList<Storage>();

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
		arch, os, vmm, hostList, time_zone, cost, costPerMem,
		costPerStorage, costPerBw);

	// 6. Finally, we need to create a HddDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new HddDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0, delay);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return datacenter;
    }

    private static HddHost createHost(final DataItem[] dataItems, final int mips, final int miops, final int ramInMb) {
	List<HddPe> hddList = new ArrayList<>();

	List<Pe> peList = new ArrayList<>();

	for (int i = 0; i < 8; i++) {
	    peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(mips)));
	}
	hddList.add(new HddPe(new PeProvisionerSimple(miops), dataItems));

	long storage = 1000000; // host storage
	int bw = 10000;

	// VmSchedulerMapVmsToPes<Pe> vmScheduler = new
	// VmSchedulerMapVmsToPes<Pe>(peList) {
	//
	// @Override
	// protected VmScheduler createSchedulerFroPe(final Pe pe) {
	// return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
	// }
	// };
	VmSchedulerTimeSharedOverSubscription vmScheduler = new VmSchedulerTimeSharedOverSubscription(peList);

	HddHost host = new HddHost(new RamProvisionerSimple(ramInMb),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		vmScheduler,
		new VmDiskScheduler(hddList));
	return host;
    }

    private static class SynchDataStatGenerator extends StatSessionGenerator {

	public SynchDataStatGenerator(final Map<String, List<Double>> asSessionParams,
		final Map<String, List<Double>> dbSessionParams,
		final int userId, final int step, final IGenerator<String[]> metadataGenerator, final DataItem... data) {
	    super(asSessionParams, dbSessionParams, userId, step, metadataGenerator, data);
	}

	@Override
	public WebSession generateSessionAt(double time) {
	    String[] meta = metadataGenerator.poll();
	    DataItem dataItem = pollRandomDataItem("EU".equals(meta[0]) ? EURO_DATA_ITEMS : US_DATA_ITEMS,
		    dataRandomiser);

	    final IGenerator<? extends WebCloudlet> appServerCloudLets = new StatGenerator(
		    GeneratorsUtil.toGenerators(asSessionParams), dataItem);
	    final IGenerator<? extends Collection<? extends WebCloudlet>> dbServerCloudLets = new CompositeGenerator<>(
		    new StatGenerator(GeneratorsUtil.toGenerators(dbSessionParams), dataItem));

	    int cloudletsNumber = asSessionParams.get(asSessionParams.keySet().toArray()[0]).size();
	    return new WebSession(appServerCloudLets,
		    dbServerCloudLets,
		    userId,
		    cloudletsNumber,
		    time + idealLength,
		    Arrays.copyOf(meta, meta.length));
	}
    }

    private static class FlushWebBroker extends WebBroker {

	public FlushWebBroker(String name, double refreshPeriod, double lifeLength, double monitoringPeriod,
		double autoscalePeriod, int dataCenterId, String... metadata) throws Exception {
	    super(name, refreshPeriod, lifeLength, monitoringPeriod, autoscalePeriod, dataCenterId, metadata);
	}

	public FlushWebBroker(String name, double refreshPeriod, double lifeLength, int dataCenterId) throws Exception {
	    super(name, refreshPeriod, lifeLength, dataCenterId);
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
	    super.processCloudletReturn(ev);

	    // Flush the memory - there are too many cloudlets
	    getCloudletReceivedList().clear();
	}
    }

    /**
     * Based on
     * http://windchill101.com/creating-a-zip-file-in-java-using-the-zip4j
     * -library/ .
     * 
     * @param dir
     * @throws ZipException
     */
    private static String archiveDir(String dir, String destinationDir) throws ZipException {
	String zipFileName = createZipFileName(dir, destinationDir);
	// Initiate ZipFile object with the path/name of the zip file.
	ZipFile zipFile = new ZipFile(zipFileName);

	// Folder to add
	String folderToAdd = dir;

	// Initiate Zip Parameters which define various properties such
	// as compression method, etc.
	ZipParameters parameters = new ZipParameters();

	// set compression method to store compression
	parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);

	// Set the compression level
	parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_MAXIMUM);

	// Add folder to the zip file
	zipFile.addFolder(folderToAdd, parameters);

	return zipFileName;

    }

    private static String createZipFileName(String dir, String destinationDir) {
	Calendar calendar = Calendar.getInstance();
	Date time = calendar.getTime();
	String name = new File(dir).getName();

	String timeTxt = DateFormat.getDateTimeInstance(
		DateFormat.SHORT, DateFormat.SHORT).format(time).replace('/', '-').replaceAll("\\s+", "_");
	String zipFileName = destinationDir + name + "_" + timeTxt + ".zip";
	return zipFileName;
    }

    private static final Function<VMex, String> F_READABLE_SUBMISSION_TIME = new Function<VMex, String>() {
	public String apply(VMex input) {
	    return TextUtil.getReadableTime(input.getSubmissionTime());
	}
    };

    private static final Function<VMex, String> F_READABLE_START_TIME = new Function<VMex, String>() {
	public String apply(VMex input) {
	    return TextUtil.getReadableTime(input.getStartTime());
	}
    };
    private static final Function<VMex, String> F_READABLE_END_TIME = new Function<VMex, String>() {
	public String apply(VMex input) {
	    return TextUtil.getReadableTime(input.getEndTime());
	}
    };

    private class BillVmFunction implements Function<VMex, String> {
	private IVmBillingPolicy policy;

	public BillVmFunction(IVmBillingPolicy policy) {
	    this.policy = policy;
	}

	public String apply(VMex input) {
	    return String.format("%.2f$", policy.bill(Arrays.asList(input), DAY).doubleValue());
	}
    }

    private static final Function<WebSession, String> F_SESSION_META = new Function<WebSession, String>() {
	public String apply(WebSession input) {
	    return Arrays.asList(input.getMetadata()).toString();
	}
    };

    private class SourceAddressFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;

	public SourceAddressFunction(GeoIP2PingERService geoService) {
	    this.geoService = geoService;
	}

	public String apply(WebSession input) {
	    return formatLocation(geoService.getMetaData(input.getSourceIP()));
	}
    }

    private class SourceCoordFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;
	private boolean latency;

	public SourceCoordFunction(GeoIP2PingERService geoService, boolean latency) {
	    this.geoService = geoService;
	    this.latency = latency;
	}

	public String apply(WebSession input) {
	    double[] res = geoService.getCoordinates(input.getSourceIP());
	    return res == null ? "N/A" : latency ? formatSingleCoord(res[0]) : formatSingleCoord(res[1]);
	}
    }

    private class DCAddressFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;

	public DCAddressFunction(GeoIP2PingERService geoService) {
	    this.geoService = geoService;
	}

	public String apply(WebSession input) {
	    return formatLocation(geoService.getMetaData(input.getServerIP()));
	}
    }

    private class DCCoordFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;
	private boolean latency;

	public DCCoordFunction(GeoIP2PingERService geoService, boolean latency) {
	    this.geoService = geoService;
	    this.latency = latency;
	}

	public String apply(WebSession input) {
	    double[] res = geoService.getCoordinates(input.getServerIP());
	    return res == null ? "N/A" : latency ? formatSingleCoord(res[0]) : formatSingleCoord(res[1]);
	}
    }

    private class LatencyFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;

	public LatencyFunction(GeoIP2PingERService geoService) {
	    this.geoService = geoService;
	}

	public String apply(WebSession input) {
	    return String.format("%.2f", geoService.latency(input.getSourceIP(), input.getServerIP()));
	}
    }

    private class URLFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;

	public URLFunction(GeoIP2PingERService geoService) {
	    this.geoService = geoService;
	}

	public String apply(WebSession input) {
	    double[] srv = geoService.getCoordinates(input.getServerIP());
	    double[] cli = geoService.getCoordinates(input.getSourceIP());
	    return srv == null || cli == null ? "N/A" :
		    String.format("http://econym.org.uk/gmap/example_plotpoints.htm?q=Client@%s,%s&q=DC@%s,%s",
			    formatSingleCoord(cli[0]),
			    formatSingleCoord(cli[1]),
			    formatSingleCoord(srv[0]),
			    formatSingleCoord(srv[1]));
	}
    }

    private class DelayLatencyFunction implements Function<WebSession, String> {
	private GeoIP2PingERService geoService;

	public DelayLatencyFunction(GeoIP2PingERService geoService) {
	    this.geoService = geoService;
	}

	public String apply(WebSession input) {
	    double latDelay = (60.0 / 7.0) * geoService.latency(input.getSourceIP(), input.getServerIP()) / 1000 * 22;
	    return String.format("%.2f", latDelay);
	}
    }

    public static String formatLocation(IPMetadata metadata) {
	String res = String.valueOf(metadata.getCountryIsoCode()) + "," + String.valueOf(metadata.getCityName());
	return res.length() < 20 ? res : res.substring(0, 16) + "...";

    }

    private static String formatSingleCoord(Double coord) {
	return coord == null ? "N/A" : String.format("%.2f", coord);
    }
}
