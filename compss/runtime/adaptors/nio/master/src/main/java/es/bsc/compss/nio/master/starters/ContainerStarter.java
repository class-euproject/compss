package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOStarterCommand;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.Ender;

import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;


public abstract class ContainerStarter extends Starter {

    protected static String INFERRED_MASTER_NAME = null;

    private static final String IP_REGEX = "^(?=.*[^\\.]$)((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4}$";


    /**
     * Generic starter for workers running over containers.
     *
     * @param nw Worker node on which to run
     */
    public ContainerStarter(NIOWorkerNode nw) {
        super(nw);
    }

    @Override
    protected void killPreviousWorker(String user, String name, int pid) throws InitNodeException {

    }

    @Override
    protected String[] generateStartCommand(int workerPort, String masterName, String hostId) throws InitNodeException {
        final String workingDir = this.nw.getWorkingDir();
        final String installDir = this.nw.getInstallDir();
        final String appDir = this.nw.getAppDir();
        String classpathFromFile = this.nw.getClasspath();
        String pythonpathFromFile = this.nw.getPythonpath();
        String libPathFromFile = this.nw.getLibPath();
        String workerName = this.nw.getName();
        int totalCPU = this.nw.getTotalComputingUnits();
        int totalGPU = this.nw.getTotalGPUs();
        int totalFPGA = this.nw.getTotalFPGAs();
        int limitOfTasks = this.nw.getLimitOfTasks();

        try {
            return new NIOStarterCommand(workerName, workerPort, masterName, workingDir, installDir, appDir,
                classpathFromFile, pythonpathFromFile, libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks,
                hostId, true).getStartCommand();
        } catch (Exception e) {
            throw new InitNodeException(e);
        }

        /*
         * try { if (Tracer.isActivated()) { return new NIOStarterCommand(workerName, workerPort, masterName,
         * workingDir, installDir, appDir, classpathFromFile, pythonpathFromFile, libPathFromFile, totalCPU, totalGPU,
         * totalFPGA, limitOfTasks, hostId).getStartCommand(); } else { String[] cmd = new
         * NIOContainerStarterCommand(workerName, workerPort, masterName, workingDir, installDir, appDir,
         * classpathFromFile, pythonpathFromFile, libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks,
         * hostId).getStartCommand(); System.out.println("CONTAINERSTARTERCOMMAND: " + String.join(" ", cmd));
         * System.out.println(); System.out.println("USECONTAINER: " + String.join(" ", useContainer(workerPort,
         * masterName, hostId))); return cmd; // return useContainer(workerPort, masterName, hostId); } } catch
         * (Exception e) { throw new InitNodeException(e); }
         */
    }

    private boolean isSameNetwork(String[] ip1, String[] ip2, int amount) {
        if (amount == 0) {
            return true;
        }
        return ip1[0].equals(ip2[0]) && isSameNetwork(Arrays.copyOfRange(ip1, 1, ip1.length),
            Arrays.copyOfRange(ip2, 1, ip2.length), amount - 1);
    }

    private synchronized String inferMasterAddress() {
        if (INFERRED_MASTER_NAME != null) {
            return INFERRED_MASTER_NAME;
        }
        try {
            String[] workerIp = this.nw.getName().split("\\.");
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface iface : Collections.list(ifaces)) {
                Iterator<InterfaceAddress> addrIt = iface.getInterfaceAddresses().listIterator();
                while (addrIt.hasNext()) {
                    InterfaceAddress addr = addrIt.next();
                    if (addr.getAddress().getHostAddress().matches(IP_REGEX)) {
                        String[] ifaceIp = addr.getAddress().getHostAddress().split("\\.");
                        if (isSameNetwork(ifaceIp, workerIp, addr.getNetworkPrefixLength() / 8)) {
                            INFERRED_MASTER_NAME = addr.getAddress().getHostAddress();
                            return INFERRED_MASTER_NAME;
                        }
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public NIONode startWorker() throws InitNodeException {
        String name = this.nw.getName();
        String user = this.nw.getUser();

        int minPort = this.nw.getConfiguration().getMinPort();
        int maxPort = this.nw.getConfiguration().getMaxPort();

        String masterName = "null";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            masterName = MASTER_NAME_PROPERTY;
        } else {
            if (nw.getName().matches(IP_REGEX)) {
                masterName = inferMasterAddress();
            }
        }

        synchronized (addressToWorkerStarter) {
            addressToWorkerStarter.put(name, this);
            LOGGER.debug("[ContainerStarter] Container starter for " + name + " registers in the hashmap");
        }

        NIONode n = this.distribute(masterName, minPort, maxPort);

        // executeCommand(user, name, getStartCommand(port, masterName));
        checkWorker(n, name);

        if (!this.workerIsReady) {
            LOGGER.info("Tried to create container with range " + minPort + "-" + maxPort + " but failed.");
        } else {
            LOGGER.info("Container created successfully with port " + n.getPort());
            Runtime.getRuntime().addShutdownHook(new Ender(this, this.nw, -1));
            return n;
        }

        String msg;

        if (this.toStop) {
            msg = "[STOP] Worker " + name + " stopped during creating because the app stopped";
        } else if (!this.workerIsReady) {
            msg = "[TIMEOUT] Could not start the NIO worker on resource " + name;
        } else {
            msg = "[UNKNOWN] Could not start the NIO worker on resource " + name;
        }
        LOGGER.error(msg);
        throw new InitNodeException(msg);
    }

    protected String imageToContainerName(String imageName) {
        String[] imageSplit = imageName.split("/");
        return imageSplit[imageSplit.length - 1].split(":")[0];
    }

    protected abstract NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException;

}
