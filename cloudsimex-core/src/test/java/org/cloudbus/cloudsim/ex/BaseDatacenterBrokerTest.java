package org.cloudbus.cloudsim.ex;

import static org.cloudbus.cloudsim.Consts.DAY;
import static org.cloudbus.cloudsim.Consts.HOUR;
import static org.cloudbus.cloudsim.Consts.MINUTE;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public abstract class BaseDatacenterBrokerTest {

    protected static final int SIM_LENGTH = DAY + HOUR / 2 + MINUTE / 3;

    protected static final int HOST_MIPS = 1000;
    protected static final int HOST_RAM = 2048;
    protected static final long HOST_STORAGE = 1000000;
    protected static final int HOST_BW = 10000;

    protected static final int VM_MIPS = 250;
    protected static final long VM_SIZE = 10000;
    protected static final int VM_RAM = 512;
    protected static final long VM_BW = 1000;

    protected DatacenterEX datacenter;
    protected DatacenterBrokerEX broker;
    protected VMex vm1;
    protected VMex vm2;

    public BaseDatacenterBrokerTest() {
        super();
    }

    public void setUp() throws Exception {
        CustomLog.configLogger(TestUtil.LOG_PROPS);

        int numBrokers = 1;
        boolean trace_flag = false;

        CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

        datacenter = createDatacenterWithSingleHostAndSingleDisk("TestDatacenter");

        // Create Broker
        broker = createBroker();

        // Create virtual machines
        List<Vm> vmlist = new ArrayList<Vm>();

        // create two VMs
        vm1 = createVM();
        vm2 = createVM();

        // add the VMs to the vmList
        vmlist.add(vm1);
        vmlist.add(vm2);

        // submit vm list to the broker
        broker.submitVmList(vmlist);
    }

    protected DatacenterBrokerEX createBroker() throws Exception {
        return new DatacenterBrokerEX("Broker", SIM_LENGTH);
    }

    protected Cloudlet createCloudlet(final double cloudletDuration) {
        UtilizationModel utilizationModel = new UtilizationModelFull();
        return new Cloudlet(Id.pollId(Cloudlet.class), (int) (VM_MIPS * cloudletDuration), 1, 0, 0, utilizationModel,
                utilizationModel, utilizationModel);
    }

    protected List<Vm> createVms(final int vmNum) {
        List<Vm> result = new ArrayList<>();
        for (int i = 0; i < vmNum; i++) {
            result.add(createVM());
        }
        return result;
    }

    protected VMex createVM() {
        int pesNumber = 1; // number of cpus
        String vmm = "Xen"; // VMM name

        return new VMex("TestVM", broker.getId(), VM_MIPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new CloudletSchedulerTimeShared());
    }

    protected DatacenterEX createDatacenterWithSingleHostAndSingleDisk(final String name) {
        List<Host> hostList = new ArrayList<Host>();
        List<Pe> peList = new ArrayList<>();

        peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
        hostList.add(new Host(Id.pollId(Host.class), new RamProvisionerSimple(HOST_RAM), new BwProvisionerSimple(
                HOST_BW), HOST_STORAGE, peList, new VmSchedulerTimeShared(peList)));

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
            datacenter = new DatacenterEX(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

}