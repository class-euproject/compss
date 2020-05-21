package es.bsc.compss.nio.master.starters.json;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DockerCreateContainerRequest {

    private boolean tty;
    private List<String> cmd;
    private String image;
    private Map<String, Map> exposedPorts;
    private HostConfig hostConfig;


    public DockerCreateContainerRequest(String[] cmd, String imageName) {
        this.tty = true;
        this.cmd = Arrays.asList(cmd);
        this.image = imageName;
        this.exposedPorts = new HashMap<>();
        this.hostConfig = new HostConfig();
    }

    public void addPort(int port) {
        exposedPorts.put(String.format("%d/tcp", port), Collections.EMPTY_MAP);
    }

    public boolean isTty() {
        return tty;
    }

    public List<String> getCmd() {
        return cmd;
    }

    public String getImage() {
        return image;
    }

    public Map<String, Map> getExposedPorts() {
        return exposedPorts;
    }

    public HostConfig getHostConfig() {
        return hostConfig;
    }


    private static class HostConfig {

        private boolean publishAllPorts;


        private HostConfig() {
            this.publishAllPorts = true;
        }

        public boolean isPublishAllPorts() {
            return publishAllPorts;
        }

        public void setPublishAllPorts(boolean publishAllPorts) {
            this.publishAllPorts = publishAllPorts;
        }
    }

}
