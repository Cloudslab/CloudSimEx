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
		|| Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Hybrid")
		|| Experiments.experimentsName.equals("test"))
	{
	    String header = "Deadline,";
	    String budgetEntry = "Budget QoS,";
	    for (Request request : requests.requests)
	    {
		header += request.getDeadline() + ",";
		budgetEntry+= request.getBudget() + ",";
	    }
	    CustomLog.printLine(header);
	    CustomLog.printLine(budgetEntry);
	}
    }

    public static void logExperimentsData(Requests requests)
    {
	CustomLog.redirectToFile("results/plots/" + Experiments.experimentsName + ".csv", true);

	if (Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Public")
		|| Experiments.experimentsName.equals("Deadline_Cost_Algorithm_Hybrid")
		|| Experiments.experimentsName.equals("test"))
	{
	    String plottingValue = requests.requests.get(0).getPolicy() + ",";
	    for (Request request : requests.requests)
		plottingValue += request.getCost() + ",";
	    CustomLog.printLine(plottingValue);
	}
    }

}
