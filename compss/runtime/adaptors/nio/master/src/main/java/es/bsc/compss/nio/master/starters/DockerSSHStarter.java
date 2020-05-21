package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.ProcessOut;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class DockerSSHStarter extends ContainerStarter {

    private final String imageName;
    private final String containerName;

    private String repository = null;
    private boolean pullIfNeeded = false;
    private boolean pushIfNeeded = false;
    private boolean failIfPull = false;

    private static final String DOCKER_SCRIPT_PATH = System.getenv("COMPSS_HOME") + File.separator + "Runtime"
        + File.separator + "scripts" + File.separator + "system" + File.separator + "adaptors" + File.separator + "nio"
        + File.separator + "docker" + File.separator + "docker_distributor.sh";


    /**
     * Generic starter for workers running over containers.
     *
     * @param nw Worker node on which to run
     */
    public DockerSSHStarter(NIOWorkerNode nw) throws InitNodeException {
        super(nw);
        LOGGER.info("Using Docker SSH starter");
        this.imageName = nw.getConfiguration().getProperty("ImageName");
        this.repository = nw.getConfiguration().getProperty("Repository");
        this.containerName = this.imageToContainerName(this.imageName) + "-" + DEPLOYMENT_ID.split("-")[0];

        if (nw.getConfiguration().getAdditionalProperties().containsKey("Mode")) {
            switch (nw.getConfiguration().getProperty("Mode").toLowerCase()) {
                case "fast":
                    LOGGER.debug("Using Docker SSH Starter in FAST mode");
                    this.pullIfNeeded = false;
                    this.failIfPull = false;
                    this.pushIfNeeded = false;
                    break;
                case "fail-fast":
                case "failfast":
                    LOGGER.debug("Using Docker SSH Starter in FAILFAST mode");
                    this.pullIfNeeded = true;
                    this.failIfPull = true;
                    this.pushIfNeeded = false;
                    break;
                case "safe":
                    LOGGER.debug("Using Docker SSH Starter in SAFE mode");
                    this.pullIfNeeded = true;
                    this.failIfPull = false;
                    this.pushIfNeeded = true;
                    break;
            }
        }

        if (nw.getConfiguration().getAdditionalProperties().containsKey("PullIfNeeded")) {
            this.pullIfNeeded = Boolean.parseBoolean(nw.getConfiguration().getProperty("PullIfNeeded"));
        }
        if (nw.getConfiguration().getAdditionalProperties().containsKey("PushIfNeeded")) {
            this.pushIfNeeded = Boolean.parseBoolean(nw.getConfiguration().getProperty("PushIfNeeded"));
        }
        if (nw.getConfiguration().getAdditionalProperties().containsKey("FailIfPull")) {
            this.failIfPull = Boolean.parseBoolean(nw.getConfiguration().getProperty("FailIfPull"));
        }

        if (this.imageName == null) {
            throw new InitNodeException("Image name must be provided");
        }
    }

    @Override
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        String tracingHostId = "NoTracingHostID";
        if (Tracer.extraeEnabled()) {
            tracingHostId = String.valueOf(NIOTracer.registerHost(this.nw.getName(), 0));
        }

        String[] command = generateStartCommand(43001, master, tracingHostId);

        List<String> cmd = new ArrayList<>();
        cmd.add(DOCKER_SCRIPT_PATH);

        cmd.add("--name");
        cmd.add(this.containerName);

        cmd.add("--image");
        cmd.add(this.imageName);

        cmd.add("--worker");
        cmd.add(this.nw.getName());

        if (this.nw.getUser() != null && !"".equals(this.nw.getUser())) {
            cmd.add("--user");
            cmd.add(this.nw.getUser());
        }

        if (this.repository != null) {
            cmd.add("--repository");
            cmd.add(this.repository);
            cmd.add("--push");
        } else if (this.pullIfNeeded) {
            cmd.add("--pull");
        }

        if (this.pushIfNeeded) {
            cmd.add("--push");
        }

        if (this.failIfPull) {
            cmd.add("--fail-if-pull");
        }

        cmd.add("--range");
        cmd.add(String.format("%d-%d:43001", minPort, maxPort));

        cmd.add("--volumes");
        cmd.add(String.format("compss:%s", "/tmp/COMPSsWorker"));

        cmd.add("--env");
        cmd.add("LD_LIBRARY_PATH=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server");

        cmd.add("--");

        cmd.add(String.join(" ", command));

        LOGGER.debug("Running script: " + String.join(" ",
            cmd.stream().map(s -> s.contains(" ") ? "\"" + s + "\"" : s).collect(Collectors.toList())));
        ProcessOut pOut = this.runRemoteCommand(cmd);
        if (pOut.getExitValue() != 0) {
            throw new InitNodeException(pOut.getError());
        }
        String[] output = pOut.getOutput().split("\n");
        LOGGER.info("Chosen port is " + output[output.length - 1]);

        return new NIONode(this.nw.getName(), Integer.parseInt(output[output.length - 1]));
    }

    @Override
    protected String[] getStopCommand(int pid) {
        return new String[0];
    }
}
