package es.bsc.compss.nio.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.types.WorkerStarterCommand;

import java.util.Optional;


public class NIOContainerStarterCommand extends WorkerStarterCommand {

    public NIOContainerStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        int totalCPU, int totalGPU, int totalFPGA, int limitOfTasks, String hostId) {
        super(workerName, workerPort, masterName, workingDir, installDir, appDir, classpathFromFile, pythonpathFromFile,
            libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks, hostId);
    }

    @Override
    public String[] getStartCommand() throws Exception {
        /*
         * ************************************************************************************************************
         * BUILD COMMAND
         * ************************************************************************************************************
         */
        String[] cmd = new String[53];

        int nextPosition = 0;

        /* SCRIPT ************************************************ */
        cmd[nextPosition++] = "/usr/bin/java";

        cmd[nextPosition++] = "-Xms1024m";
        cmd[nextPosition++] = "-Xmx1024m";
        cmd[nextPosition++] = "-Xmn400m";
        cmd[nextPosition++] = "-XX:+PerfDisableSharedMem";
        cmd[nextPosition++] = "-XX:-UsePerfData";
        cmd[nextPosition++] = "-XX:+UseG1GC";
        cmd[nextPosition++] = "-XX:+UseThreadPriorities";
        cmd[nextPosition++] = "-XX:ThreadPriorityPolicy=42";
        cmd[nextPosition++] =
            "-Dlog4j.configurationFile=/opt/COMPSs//Runtime/configuration/log/COMPSsWorker-log4j.debug";
        cmd[nextPosition++] = "-Dcompss.python.interpreter=" + this.pythonInterpreter;
        cmd[nextPosition++] = "-Dcompss.python.version=" + this.pythonVersion;
        cmd[nextPosition++] =
            "-Dcompss.python.virtualenvironment=" + Optional.ofNullable(this.pythonVirtualEnvironment).orElse("null");
        cmd[nextPosition++] = "-Dcompss.python.propagate_virtualenvironment=" + this.pythonPropagateVirtualEnvironment;
        cmd[nextPosition++] = "-Dcompss.worker.removeWD=false";
        cmd[nextPosition++] = "-Djava.library.path=" + this.workerLibPath;
        cmd[nextPosition++] = "-cp";
        cmd[nextPosition++] =
            this.workerClasspath + ":/opt/COMPSs//Runtime/adaptors/nio/worker/compss-adaptors-nio-worker.jar";

        cmd[nextPosition++] = "es.bsc.compss.nio.worker.NIOWorker";

        cmd[nextPosition++] = this.workerDebug;
        // Internal parameters
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = workerName;
        cmd[nextPosition++] = String.valueOf(workerPort);
        cmd[nextPosition++] = masterName == null || masterName.isEmpty() ? "null" : masterName;
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);
        cmd[nextPosition++] = String.valueOf(Comm.getStreamingPort());

        // Worker parameters
        cmd[nextPosition++] = String.valueOf(totalCPU);
        cmd[nextPosition++] = String.valueOf(totalGPU);
        cmd[nextPosition++] = String.valueOf(totalFPGA);

        // affinity
        cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(GPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(FPGA_AFFINITY);
        cmd[nextPosition++] = String.valueOf(limitOfTasks);

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = lang;
        cmd[nextPosition++] = workingDir;
        cmd[nextPosition++] = installDir;

        cmd[nextPosition++] = cmd[2];
        cmd[nextPosition++] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[nextPosition++] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[nextPosition++] = workerPythonpath.isEmpty() ? "null" : workerPythonpath;

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.getLevel());
        cmd[nextPosition++] = NIOTracer.getExtraeFile();
        cmd[nextPosition++] = hostId;

        // Storage parameters
        cmd[nextPosition++] = storageConf;
        cmd[nextPosition++] = executionType;

        // persistent_c parameter
        cmd[nextPosition++] = workerPersistentC;

        // Python interpreter parameter
        cmd[nextPosition++] = pythonInterpreter;
        // Python interpreter version
        cmd[nextPosition++] = pythonVersion;
        // Python virtual environment parameter
        cmd[nextPosition++] = pythonVirtualEnvironment;
        // Python propagate virtual environment parameter
        cmd[nextPosition++] = pythonPropagateVirtualEnvironment;
        // Python use MPI worker parameter
        cmd[nextPosition++] = pythonMpiWorker;

        if (cmd.length != nextPosition) {
            throw new Exception(
                "ERROR: Incorrect number of parameters. Expected: " + cmd.length + ". Got: " + nextPosition);
        }

        return cmd;
    }

    @Override
    public void setScriptName(String scriptName) {
        // do nothing
    }

    @Override
    public String getWorkingDirectory() {
        return this.workingDir;
    }
}
