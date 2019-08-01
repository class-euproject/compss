package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.master.NIOWorkerNode;

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
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        // TODO: minPort?????!?!?!?!
        final String[] command = getStartCommand(minPort, master);
        String containerId = "compss-" + DEPLOYMENT_ID.split("-")[0] + "-" + minPort;
        List<String> cmd = new ArrayList<>();
        cmd.add(LXC_SCRIPT_PATH);
        cmd.add("--name");
        cmd.add(containerId);
        cmd.add("--image");
        cmd.add(this.imageName);
        cmd.add("--master");
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            cmd.add(MASTER_NAME_PROPERTY);
        } else {
            cmd.add("localhost");
        }
        cmd.add("--worker");
        cmd.add(this.nw.getName());
        cmd.add("--storage");
        cmd.add("compss:compss:/tmp/COMPSsWorker");

        if (this.nw.getUser() != null && !this.nw.getUser().equals("")) {
            cmd.add("--user");
            cmd.add(this.nw.getUser());
        }

        if (this.pullIfNeeded) {
            cmd.add("--pull");
        }

        cmd.add("--ports");
        cmd.add(String.format("%1$d:%1$d", minPort));
        cmd.add("--");
        cmd.addAll(Arrays.asList("/bin/sh", "-c", "mkdir -p " + nw.getWorkingDir() + "/log && " + "mkdir -p "
            + nw.getWorkingDir() + "/jobs && " + String.join(" ", command) + " > /unai.txt"));

        LOGGER.debug("Running script: " + String.join(" ",
            cmd.stream().map(s -> s.contains(" ") ? "\"" + s + "\"" : s).collect(Collectors.toList())));
        this.runRemoteCommand(cmd);

        // return containerId;
        return new NIONode(this.nw.getName(), minPort);
    }
}
