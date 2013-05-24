package org.cloudbus.cloudsim.ex.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.yaml.snakeyaml.Yaml;

public class YamlFile extends Yaml {

    static Yaml yaml = new Yaml();

    public static Cloud getCloudFromYaml(String fileName) {
	return (Cloud) getObjectFromYaml(fileName);
    }

    public static Experiment getRequestsFromYaml(String fileName) {
	return (Experiment) getObjectFromYaml(fileName);
    }

    private static Object getObjectFromYaml(String fileName) {
	InputStream document = null;
	try {
	    document = new FileInputStream(new File(fileName));
	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return yaml.load(document);
    }

}
