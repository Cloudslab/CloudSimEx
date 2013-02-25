package org.cloudbus.cloudsim.ex.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
import org.yaml.snakeyaml.Yaml;

public class YamlFile extends Yaml {

	static Yaml yaml = new Yaml();

	public static Cloud getCloudFromYaml(String fileName) {
		try {
			if (Cloud.brokerID == -1)
				throw new Exception("brokerID is not set");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (Cloud) getObjectFromYaml(fileName);
	}

	public static Requests getRequestsFromYaml(String fileName) {
		try {
			if (Cloud.brokerID == -1)
				throw new Exception("brokerID is not set");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (Requests) getObjectFromYaml(fileName);
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
