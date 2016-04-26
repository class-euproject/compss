package integratedtoolkit.types;

public class Profile {

    protected long executions;

    private long startTime;
    private long minTime;
    private long averageTime;
    private long maxTime;

    public Profile() {
        this.executions = 0;
        this.minTime = Long.MAX_VALUE;
        this.averageTime = 100;
        this.maxTime = Long.MIN_VALUE;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void end() {
        averageTime = System.currentTimeMillis() - startTime;
        minTime = averageTime;
        maxTime = averageTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getExecutionCount() {
        return executions;
    }

    public long getMinExecutionTime() {
        return minTime;
    }

    public long getAverageExecutionTime() {
        return averageTime;
    }

    public long getMaxExecutionTime() {
        return maxTime;
    }

    public void accumulate(Profile profile) {
        minTime = Math.min(minTime, profile.minTime);
        averageTime = (profile.averageTime + executions * averageTime) / (executions + 1);
        maxTime = Math.max(maxTime, profile.maxTime);
        executions += profile.executions;
    }

}
