package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class Experiments
{
    public List<Experiment> experiments = new ArrayList<Experiment>();
    public static String experimentsName;

    public static void logExperimentsHeader(Requests requests)
    {
	// Experiments Plotting
	Experiments.experimentsName = Properties.EXPERIMENTS.getProperty().split(".yaml")[0];
	CustomLog.redirectToFile("results/plots/" + Experiments.experimentsName + ".csv");

	if (Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Public")
		|| Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Hybrid"))
	{
	    String header = "Deadline,";
	    for (Request request : requests.requests)
		header += request.getDeadline() + ",";
	    CustomLog.printLine(header);
	}
    }

    public static void logExperimentsData(Requests requests)
    {
	CustomLog.redirectToFile("results/plots/" + Experiments.experimentsName + ".csv", true);

	if (Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Public")
		|| Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Hybrid"))
	{
	    String plottingValue = requests.requests.get(0).getPolicy() + ",";
	    for (Request request : requests.requests)
		plottingValue += request.getCost() + ",";
	    CustomLog.printLine(plottingValue);
	}
    }

}
