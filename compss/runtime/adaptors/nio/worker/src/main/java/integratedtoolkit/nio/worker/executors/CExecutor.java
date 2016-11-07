package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutor extends ExternalExecutor {

    private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
    private static final String COMMONS_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "commons" + File.separator
            + "lib";
    private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";


    public CExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe, TaskResultReader resultReader) {
        super(nw, pool, queue, writePipe, resultReader);
    }

    @Override
    public ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits) {
        ArrayList<String> lArgs = new ArrayList<>();

        // NX_ARGS string built from the Resource Description
        StringBuilder reqs = new StringBuilder();
        int numCUs = nt.getResourceDescription().getTotalCPUComputingUnits();
        reqs.append("NX_ARGS='--smp-cpus=").append(numCUs);

        // Debug mode on
        if (workerDebug) {
            reqs.append(" --summary");
        }
        reqs.append("' ");
           
        // Taskset string to bind the job
		StringBuilder taskset = new StringBuilder();
		taskset.append("taskset -c ");
		for (int i = 0; i < (numCUs - 1); i++){
			taskset.append(assignedCoreUnits[i]).append(",");
		}
		
		taskset.append(assignedCoreUnits[numCUs - 1]).append(" ");

        lArgs.add(reqs.toString() + taskset.toString() + nw.getAppDir() + WORKER_C_RELATIVE_PATH);

        return lArgs;
    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        Map<String, String> env = new HashMap<>();
        String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }

        // Add C and commons libs
        ldLibraryPath.concat(":" + nw.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath.concat(":" + nw.getInstallDir() + COMMONS_LIB_RELATIVE_PATH);

        env.put("LD_LIBRARY_PATH", ldLibraryPath);
        return env;
    }

}
