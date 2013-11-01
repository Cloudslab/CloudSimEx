import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

public class Parser {

    private static String unknownCharPattern;
    static {
	StringBuilder sb = new StringBuilder("[^;");
	for (char c : ";wd.:-s()[]{}'\"@/\\".toCharArray()) {
	    sb.append("\\" + c);
	}
	sb.append("]");
	unknownCharPattern = sb.toString();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
	// args = new String[]{"/home/nikolay/Dev/Eclipse_WS_Uni/cloudsimex/cloudsimex-experiments/results/multi-cloud-stat/wldf(baseline)-200-n-20"};

	for (String arg : args) {
	    File dir = new File(arg);
	    if (!dir.isDirectory()) {
		continue;
	    }

	    System.out.println("Processing files in: " + arg);
	    FilenameFilter filter = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
		    return name.endsWith(".csv") && name.startsWith("Sessions-Broker") ;
		}
	    };

	    for (File inputFile : dir.listFiles(filter)) {
		File outputFile = new File(inputFile.getAbsolutePath() + "_tmp");
		int buffSize = 100_000;

		System.out.println("\tFile: " + inputFile.getName());

		int lineNum = 1;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true), buffSize);
			BufferedReader br = new BufferedReader(new FileReader(inputFile), buffSize)) {
		    String line;
		    double[] lastFinishTime = inputFile.getName().startsWith("Sessions") ? new double[] { 0 } : null;
		    while ((line = br.readLine()) != null) {
			bw.write(convertLine(line, lineNum == 1, lineNum == 1 ? null : lastFinishTime));
			bw.newLine();
			lineNum++;
			if ((lineNum % 50_000) == 0) {
			    System.out.println("\t\tProcessed " + lineNum + " lines ...");
			}
		    }
		}

		if (inputFile.delete()) {
		    outputFile.renameTo(inputFile);
		}
		System.out.println("\tFile: " + inputFile.getName() + " is done");
		System.out.println();
	    }
	}
    }

    public static void mainTest(String[] args) {
	System.out
		.println(convertLine(
			"RealTime;	      Time;	 0: 0: 0: 0;	SessionId;AppVmId;   ReadableStartTime; StartTime;FinishTime;  IdealEnd;     Delay;Complete;Failed;            SourceIP;            ServerIP;            LatDelay;                Meta;             Latency;        UserLocation;             UserLat;             UserLon;          DCLocation;               DCLat;              DCLong;",
			true, null));

	double[] d = new double[] { 0 };
	String line1 = "16:20:26;	      0.00;	 0: 0: 0: 0;	      368;      5;          0: 0: 0: 0;         0.00;      5.00;   1476.00;      0.00;   false;  true;       178.79.230.67;         5.149.168.0;                3.62;                [US];               19.21;             IT,null;               42.83;               12.83;           IE,Dublin;               53.33;               -6.25;";
	String line2 = "16:20:26;	      0.00;	 0: 0: 0: 0;	      702;      5;          0: 0: 0: 0;         6.00;      2.00;   1570.00;      0.00;   false;  true;      62.227.170.235;         5.149.168.0;                2.63;                [US];               13.97;       DE,Oftersheim;               49.37;                8.58;           IE,Dublin;               53.33;               -6.25;";
	String line3 = "16:20:26;	      0.00;	 0: 0: 0: 0;	      702;      5;          0: 0: 0: 0;         0.00;      0.00;   1570.00;      0.00;   false;  true;      62.227.170.235;         5.149.168.0;                2.63;                [US];               13.97;       DE,Oftersheim;               49.37;                8.58;           IE,Dublin;               53.33;               -6.25;";
	String line4 = "16:20:26;	      0.00;	 0: 0: 0: 0;	      702;      5;          0: 0: 0: 0;         0.00;      0.00;   1570.00;      0.00;   false;  true;      62.227.170.235;         5.149.168.0;                2.63;                [US];               13.97;       DE,Oftersheim;               49.37;                8.58;           IE,Dublin;               53.33;               -6.25;";

	System.out.println(line1);
	System.out.println(line2);
	System.out.println(line3);
	System.out.println(line4);

	System.out.println(convertLine(line1, false, d));
	System.out.println(convertLine(line2, false, d));
	System.out.println(convertLine(line3, false, d));
	System.out.println(convertLine(line4, false, d));
    }

    private static String convertLine(final String line, boolean header, double[] times) {
	String res = line.
		replaceAll("http://.*$", "").
		replaceAll("([^;])(\\t+)", "$1;$2").
		replaceAll(unknownCharPattern, "0.00").
		replaceAll("(null|[A-Z]);(\\w|\\.)", "$1,$2");

	// Handle 0 start time of rejected sessions.
	if (times != null) {
	    int i = 0;
	    int idx = -1;
	    do {
		idx = res.indexOf(';', idx + 1);
		i++;
	    } while (i < 6);
	    int starTimeIdx = res.indexOf(';', idx + 1);
	    int finishTimeIdx = res.indexOf(';', starTimeIdx + 1);
	    double startTime = Double.parseDouble(res.substring(idx + 1, starTimeIdx).trim());
	    double finishTime = Double.parseDouble(res.substring(starTimeIdx + 1, finishTimeIdx).trim());

	    if (startTime == 0) {
		startTime = times[0];
		res = res.substring(0, idx + 1) + String.format("    %.2f", startTime) + res.substring(starTimeIdx);
	    }
	    if (finishTime > 0) {
		times[0] = finishTime;
	    }
	}

	if (header) {
	    res = res.replaceAll("^.*?;(\\s*).*?;(\\s*).*?;", "RealTime;$1Time;$2RedableTime;")
		    .replaceAll("URL.*$", "");
	}
	return res;
    }
}
