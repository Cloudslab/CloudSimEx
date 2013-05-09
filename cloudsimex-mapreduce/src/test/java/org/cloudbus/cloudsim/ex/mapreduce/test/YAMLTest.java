package org.cloudbus.cloudsim.ex.mapreduce.test;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class YAMLTest {

    // @Test
    // public void testLoadCloudFile() throws FileNotFoundException {
    // // First step: Initialize the CloudSim package. It should be called
    // // before creating any entities.
    // int num_user = 1; // number of cloud users
    // Calendar calendar = Calendar.getInstance();
    // boolean trace_flag = false; // mean trace events
    //
    // // Initialize the CloudSim library
    // CloudSim.init(num_user, calendar, trace_flag);
    //
    // Yaml yaml = new Yaml();
    //
    // InputStream document = new FileInputStream(new File("Cloud.yaml"));
    // Cloud obj = (Cloud) yaml.load(document);
    // assertNotNull( obj.publicCloudDatacenters.get(0).getName());
    // assertNotNull(obj.publicCloudDatacenters.get(0).getHostList().size());
    // assertNotNull(obj.publicCloudDatacenters.get(0).getHostList().get(0).getPeList().size());
    // assertNotNull(obj.publicCloudDatacenters.get(0).getHostList().get(0).getPeList().get(0).getMips());
    //
    // assertNotNull(obj.publicCloudDatacenters.get(0).vmTypes.get(0).name);
    // assertNotNull(obj.privateCloudDatacenters.get(1).vmTypes.get(0).name);
    //
    // assertNotNull(obj.dataSources.get(0).getName());
    //
    // assertNotNull(obj.throughputs_vm_vm.get(0).get(2));
    //
    // System.out.println(yaml.dump(obj));
    // }
    //
    // @Test
    // public void testLoadRequestsFile() throws FileNotFoundException {
    // // First step: Initialize the CloudSim package. It should be called
    // // before creating any entities.
    // int num_user = 1; // number of cloud users
    // Calendar calendar = Calendar.getInstance();
    // boolean trace_flag = false; // mean trace events
    //
    // // Initialize the CloudSim library
    // CloudSim.init(num_user, calendar, trace_flag);
    //
    // Yaml yaml = new Yaml();
    //
    // InputStream document = new FileInputStream(new File("Requests.yaml"));
    // Requests obj = (Requests) yaml.load(document);
    //
    // assertNotNull(obj.requests.get(0).userClass);
    //
    // System.out.println(yaml.dump(obj));
    // }
    //
    // @Test
    // public void testLoadJobFile() throws FileNotFoundException {
    // // First step: Initialize the CloudSim package. It should be called
    // // before creating any entities.
    // int num_user = 1; // number of cloud users
    // Calendar calendar = Calendar.getInstance();
    // boolean trace_flag = false; // mean trace events
    //
    // // Initialize the CloudSim library
    // CloudSim.init(num_user, calendar, trace_flag);
    //
    // Yaml yaml = new Yaml();
    //
    // InputStream document = new FileInputStream(new
    // File("Jobs/MapReduce_3_2.yaml"));
    // Job obj = (Job) yaml.load(document);
    //
    // assertNotNull(obj.mapTasks.get(0).getNumberOfPes());
    // assertNotNull(obj.reduceTasks.get(0).getNumberOfPes());
    //
    // System.out.println(yaml.dump(obj));
    // }

    @Test
    public void testDumpYAML() throws Exception
    {
	String[][] test = new String[2][2];
	test[0][0] = "test0";
	test[0][1] = "test1";
	test[1][0] = "test2";
	test[1][1] = "test3";
	String output = new Yaml().dump(test);
	System.out.println(output);

    }

}
