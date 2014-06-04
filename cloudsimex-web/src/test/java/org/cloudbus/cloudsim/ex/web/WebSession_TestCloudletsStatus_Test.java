package org.cloudbus.cloudsim.ex.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.DatacenterEX;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.disk.HddCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.ex.disk.HddDataCenter;
import org.cloudbus.cloudsim.ex.disk.HddHost;
import org.cloudbus.cloudsim.ex.disk.HddPe;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.disk.VmDiskScheduler;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.ex.web.workload.brokers.WebBroker;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class WebSession_TestCloudletsStatus_Test {

    private static final int ITEM_SIZE = 5;
    private static final double DELTA = 0.01;

    protected DatacenterEX datacenter;
    protected WebBroker broker;
    protected HddVm vm1;
    protected HddVm vm2;
    protected DataItem dataItem1;

    private static final int HOST_MIPS = 1000;
    private static final int HOST_MIOPS = 100;
    private static final int HOST_RAM = 2048; // host memory (MB)
    private static final long HOST_STORAGE = 1000000; // host storage
    private static final int HOST_BW = 10000;

    private static final int VM_MIPS = 250;
    private static final long VM_SIZE = 10000;
    private static final int VM_RAM = 512;
    private static final long VM_BW = 1000;

    @Before
    public void setUp() throws Exception {
        CustomLog.configLogger(TestUtil.LOG_PROPS);

        int numBrokers = 1;
        boolean trace_flag = false;

        CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

        datacenter = createDatacenterWithSingleHostAndSingleDisk("TestDatacenter");

        // Create Broker
        broker = new WebBroker("Broker", 5, 100, datacenter.getId());

        // Create virtual machines
        List<Vm> vmlist = new ArrayList<Vm>();

        int pesNumber = 1; // number of cpus
        String vmm = "Xen"; // VMM name

        // create two VMs
        vm1 = new HddVm("Test", broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new HddCloudletSchedulerTimeShared(), new Integer[0]);
        vm2 = new HddVm("Test", broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new HddCloudletSchedulerTimeShared(), new Integer[0]);

        // add the VMs to the vmList
        vmlist.add(vm1);

        // submit vm list to the broker
        broker.submitVmList(vmlist);
    }

    @Test
    public void testWebSessions() {
        broker.submitVmList(Arrays.asList(vm2));

        double factor1 = 0.5;
        double factor2 = 0.25;
        WebCloudlet asCl1 = new WebCloudlet(0, (int) (VM_MIPS * factor1), 0, 10, broker.getId(), false, null);
        WebCloudlet asCl2 = new WebCloudlet(0, (int) (VM_MIPS * factor2), 0, 10, broker.getId(), false, null);

        WebCloudlet dbCl1 = new WebCloudlet(0, (int) (VM_MIPS * factor2), (int) (HOST_MIOPS * factor2), 10,
                broker.getId(), false, dataItem1);
        WebCloudlet dbCl2 = new WebCloudlet(0, (int) (VM_MIPS * factor1), (int) (HOST_MIOPS * factor1), 10,
                broker.getId(), false, dataItem1);

        IGenerator<WebCloudlet> generatorAS = new IterableGenerator<>(Arrays.asList(asCl1, asCl2));
        CompositeGenerator<WebCloudlet> generatorDB = new CompositeGenerator<>(new IterableGenerator<>(Arrays.asList(
                dbCl1, dbCl2)));

        ILoadBalancer balancer = new SimpleWebLoadBalancer(1, "127.0.0.1", Arrays.asList(vm1),
                new SimpleDBBalancer(vm2));
        broker.addLoadBalancer(balancer);

        WebSession session = new WebSession(generatorAS, generatorDB, broker.getId(), 2, -1);
        // session.setAppVmId(vm1.getId());
        // session.setDbBalancer(vm2.getId());

        double delay = 1; // Give time for the VM to boot...
        broker.submitSessionsAtTime(Arrays.asList(session), balancer.getAppId(), delay);

        CloudSim.startSimulation();
        List<HddCloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        assertEquals(4, resultList.size());

        assertEquals(asCl1.getExecStartTime(), dbCl1.getExecStartTime(), DELTA);
        assertEquals(asCl2.getExecStartTime(), dbCl2.getExecStartTime(), DELTA);

        assertTrue(asCl1.isFinished());
        assertTrue(asCl2.isFinished());
        assertTrue(dbCl1.isFinished());
        assertTrue(dbCl2.isFinished());

        double asCl1Len = asCl1.getFinishTime() - asCl1.getExecStartTime();
        double asCl2Len = asCl2.getFinishTime() - asCl2.getExecStartTime();
        double dbCl1Len = dbCl1.getFinishTime() - dbCl1.getExecStartTime();
        double dbCl2Len = dbCl2.getFinishTime() - dbCl2.getExecStartTime();

        assertTrue(asCl1Len > dbCl1Len);
        assertTrue(asCl2Len < dbCl2Len);
    }

    private DatacenterEX createDatacenterWithSingleHostAndSingleDisk(final String name) {
        List<Host> hostList = new ArrayList<Host>();

        List<Pe> peList = new ArrayList<>();
        List<HddPe> hddList = new ArrayList<>();

        peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
        dataItem1 = new DataItem(ITEM_SIZE);
        hddList.add(new HddPe(new PeProvisionerSimple(HOST_MIOPS), dataItem1));

        hostList.add(new HddHost(new RamProvisionerSimple(HOST_RAM), new BwProvisionerSimple(HOST_BW), HOST_STORAGE,
                peList, hddList, new VmSchedulerTimeShared(peList), new VmDiskScheduler(hddList)));

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList<Storage> storageList = new LinkedList<Storage>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone,
                cost, costPerMem, costPerStorage, costPerBw);

        DatacenterEX datacenter = null;
        try {
            datacenter = new HddDataCenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList,
                    0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

}
