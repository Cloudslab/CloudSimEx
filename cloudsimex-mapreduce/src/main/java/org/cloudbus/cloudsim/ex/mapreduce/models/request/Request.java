package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;


import org.yaml.snakeyaml.Yaml;

public class Request {
	public Double budget;
	public int deadline;
	public Job job;
	public UserClass userClass;
	
	public Request(Double budget, int deadline, String jobFile, UserClass userClass) {
		this.budget = budget;
		this.deadline = deadline;
		this.userClass = userClass;
		job = readJobYAML(jobFile);
	}

	private Job readJobYAML(String jobFile) {
		Job job = new Job();
		
		Yaml yaml = new Yaml();
		InputStream document = null;
		
		try {
			document = new FileInputStream(new File(jobFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job = (Job) yaml.load(document);
		
		return job;
	}
	
	
}
