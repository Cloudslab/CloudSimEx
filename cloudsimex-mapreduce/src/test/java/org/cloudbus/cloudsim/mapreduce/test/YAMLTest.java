package org.cloudbus.cloudsim.mapreduce.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.mapreduce.models.Cloud;
import org.cloudbus.cloudsim.mapreduce.models.cloud.DataSource;
import org.junit.Test;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import junit.framework.TestCase;

public class YAMLTest extends TestCase {
	
	@Test
	public void testLoad() throws FileNotFoundException {
		
		DumperOptions options = new DumperOptions();
		options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
		
		
		Constructor constructor = new Constructor(Cloud.class);
		TypeDescription cloudDescription = new TypeDescription(Cloud.class);
		cloudDescription.putListPropertyType("dataSources", DataSource.class);
		constructor.addTypeDescription(cloudDescription);
		Yaml yaml = new Yaml(constructor);
		
		InputStream document = new FileInputStream(new File("Cloud.yaml"));
		Object obj = yaml.load(document);
		System.out.println(obj);
		System.out.println(yaml.dump(obj));
		
		/*
		document = new FileInputStream(new File("Cloud.yaml"));
		Object map = yaml.load(document);
		System.out.println(map.getClass().toString());
		Map<String, String> m = (Map<String, String>) map;
		System.out.println(m.keySet().size());
		Iterator<String> iter = m.keySet().iterator();
		System.out.println(m.values().toString());
	    //System.out.println(yaml.dump(yaml.load(document)));
	    */	
	    
	    //document = new FileInputStream(new File("Requests.yaml"));
	    //System.out.println(yaml.dump(yaml.load(document)));
	}
}
