package org.cloudbus.cloudsim.ex.web.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.sessions.ISessionGenerator;

/**
 * 
 * Generates a same-sized set of sessions whenever asked to. One can specify the
 * time period for which this generator is active, or the maximum number of
 * times generations it can be used. If these are not specified the generator
 * can be used "endlessly".
 * 
 * @author nikolay.grozev
 * 
 */
public class SimpleWorkloadGenerator implements IWorkloadGenerator {

    private final int sessionsNumber;
    private final ISessionGenerator sessGen;

    private Double startTime = null;
    private Double endTime = null;
    private Integer count = null;

    /**
     * Constr. Generates a same-sized set of sessions whenever asked to and if
     * the time is in the interval [startTime, endTime] and less than count
     * generations have been done.
     * 
     * @param sessionsNumber
     *            - the size of the result set. Must be positive.
     * @param sessGen
     *            - the generator of the sessions which is used.
     * @param startTime
     *            - the start time. null is considered as minus infinity.
     * @param endTime
     *            - the end time. null is considered as infinity.
     * @param count
     *            - the number of generations to do. Null is infinity.
     */
    public SimpleWorkloadGenerator(final int sessionsNumber, final ISessionGenerator sessGen, final Double startTime,
            final Double endTime, final Integer count) {
        super();
        this.sessionsNumber = sessionsNumber;
        this.sessGen = sessGen;
        this.startTime = startTime;
        this.endTime = endTime;
        this.count = count;
    }

    /**
     * Constr. Always generates a same-sized set of sessions whenever asked to.
     * 
     * @param sessionsNumber
     *            - the size of the set. Must be positive.
     * @param sessGen
     *            - the generator of the sessions which is used.
     */
    public SimpleWorkloadGenerator(final int sessionsNumber, final ISessionGenerator sessGen) {
        super();
        this.sessionsNumber = sessionsNumber;
        this.sessGen = sessGen;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.ex.web.workload.IWorkloadGenerator#generateSessions
     * (double, double)
     */
    @Override
    public Map<Double, List<WebSession>> generateSessions(final double startTime, final double periodLen) {
        boolean generate = this.startTime == null || this.startTime <= startTime;
        generate &= this.endTime == null || this.endTime >= startTime;
        generate &= this.count == null || this.count > 0;

        Map<Double, List<WebSession>> result = new HashMap<>();
        if (generate) {
            this.count--;
            for (int i = 0; i < sessionsNumber; i++) {
                double startAt = startTime;// + Math.random() * periodLen;
                if (!result.containsKey(startAt)) {
                    result.put(startAt, new ArrayList<WebSession>());
                }
                result.get(startAt).add(sessGen.generateSessionAt(startAt));
            }
        }
        return result;
    }
}
