package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.ProcessOut;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class DockerStarter extends ContainerStarter {

    private String imageName = null;
    private String repository = null;
    private boolean pullIfNeeded = false;

    private static final String DOCKER_SCRIPT_PATH = System.getenv("COMPSS_HOME") + File.separator + "Runtime"
        + File.separator + "scripts" + File.separator + "system" + File.separator + "adaptors" + File.separator + "nio"
        + File.separator + "docker" + File.separator + "docker_distributor.sh";


    /**
     * Starts a node on a Docker container.
     * 
     * @param node The node
     * @throws InitNodeException When an error occurs when initializing the node
     */
    public DockerStarter(NIOWorkerNode node) throws InitNodeException {
        super(node);
        this.imageName = node.getConfiguration().getProperty("ImageName");
        this.repository = node.getConfiguration().getProperty("Repository");
        if (node.getConfiguration().getAdditionalProperties().containsKey("PullIfNeeded")) {
            this.pullIfNeeded = Boolean.getBoolean(node.getConfiguration().getProperty("PullIfNeeded"));
        }

        if (this.imageName == null) {
            throw new InitNodeException("Image name must be provided");
        }
    }

    @Override
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        final String[] command = getStartCommand(43001, master);

        // String containerId = this.imageToContainerName(this.imageName);
        List<String> cmd = new ArrayList<>();
        cmd.add(DOCKER_SCRIPT_PATH);
        // cmd.add("--name"); cmd.add(containerId);
        cmd.add("--name");
        cmd.add(this.imageToContainerName(this.imageName) + "-" + DEPLOYMENT_ID.split("-")[0]);
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

        // cmd.add("--reuse");
        cmd.add("--range");
        cmd.add(String.format("%d-%d:43001", minPort, maxPort));

        cmd.add("--volumes");
        cmd.add(String.format("compss:%s", "/tmp/COMPSsWorker"));
        cmd.add("--");
        cmd.add("mkdir -p " + this.nw.getWorkingDir() + "/jobs && " + "mkdir -p " + this.nw.getWorkingDir() + "/log && "
            + String.join(" ", command));

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

    private String imageToContainerName(String imageName) {
        String[] imageSplit = imageName.split("/");
        return imageSplit[imageSplit.length - 1].split(":")[0];
    }

    @Override
    protected void killPreviousWorker(String user, String name, int pid) throws InitNodeException {

    }
}
