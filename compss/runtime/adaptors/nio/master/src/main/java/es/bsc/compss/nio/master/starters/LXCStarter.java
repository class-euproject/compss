package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.ProcessOut;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class LXCStarter extends ContainerStarter {

    private static final String LXC_SCRIPT_PATH = System.getenv("COMPSS_HOME") + File.separator + "Runtime"
        + File.separator + "scripts" + File.separator + "system" + File.separator + "adaptors" + File.separator + "nio"
        + File.separator + "lxc" + File.separator + "lxc_distributor.sh";

    private String imageName;
    private boolean pullIfNeeded;
    private String containerId;


    /**
     * Starts a node on a LXC container.
     *
     * @param node The node
     * @throws InitNodeException When an error occurs when initializing the node
     */
    public LXCStarter(NIOWorkerNode node) throws InitNodeException {
        super(node);
        this.imageName = nw.getConfiguration().getProperty("ImageName");
        this.pullIfNeeded = Boolean.getBoolean(nw.getConfiguration().getProperty("PullIfNeeded"));

        if (this.imageName == null) {
            throw new InitNodeException("Image name must be provided");
        }
    }

    @Override
    protected String[] getStopCommand(int pid) {
        return new String[] { "lxc",
            "stop",
            this.containerId };
    }

    @Override
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        final String[] command = getStartCommand(43001, master);
        this.containerId = "compss-" + DEPLOYMENT_ID.split("-")[0];
        List<String> cmd = new ArrayList<>();
        cmd.add(LXC_SCRIPT_PATH);
        cmd.add("--name");
        cmd.add(this.containerId);
        cmd.add("--image");
        cmd.add(this.imageName);
        cmd.add("--master");
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            cmd.add(MASTER_NAME_PROPERTY);
        } else if (INFERRED_MASTER_NAME != null) {
            cmd.add(INFERRED_MASTER_NAME);
        } else {
            cmd.add("localhost");
        }
        cmd.add("--worker");
        cmd.add(this.nw.getName());
        cmd.add("--storage");
        cmd.add("compss:compss:/tmp/COMPSsWorker");
        cmd.add("--as-public-server");

        if (this.nw.getUser() != null && !this.nw.getUser().equals("")) {
            cmd.add("--user");
            cmd.add(this.nw.getUser());
        }

        if (this.pullIfNeeded) {
            cmd.add("--pull");
        }

        // cmd.add("--ports"); cmd.add(String.format("%1$d:%1$d", minPort));
        cmd.add("--range");
        cmd.add(String.format("%d-%d:43001", minPort, maxPort));
        cmd.add("--");
        cmd.addAll(Arrays.asList("/bin/sh", "-c", "mkdir -p " + nw.getWorkingDir() + "/log && " + "mkdir -p "
            + nw.getWorkingDir() + "/jobs && " + String.join(" ", command)));

        LOGGER.debug("Running script: " + String.join(" ",
            cmd.stream().map(s -> s.contains(" ") ? "\"" + s + "\"" : s).collect(Collectors.toList())));
        ProcessOut pOut = this.runRemoteCommand(cmd);
        if (pOut.getExitValue() != 0) {
            throw new InitNodeException(pOut.getError());
        }
        String[] output = pOut.getOutput().split("\n");
        LOGGER.info("Chosen port is " + output[output.length - 1]);

        // return containerId;
        return new NIONode(this.nw.getName(), Integer.parseInt(output[output.length - 1]));
    }
}
