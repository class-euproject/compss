package es.bsc.compss.nio.master.configuration.rotterdam.config;

public class PortConfig {

    private Integer containerPort;
    private Integer hostPort;
    private String protocol;


    /**
     * Utility class for port configuration (de)serialization in Rotterdam request/response body.
     * 
     * @param containerPort Port within the container
     * @param hostPort Corresponding port in host
     * @param protocol Protocol (usually just TCP)
     */
    public PortConfig(Integer containerPort, Integer hostPort, String protocol) {
        this.containerPort = containerPort;
        this.hostPort = hostPort;
        this.protocol = protocol;
    }

    public Integer getContainerPort() {
        return containerPort;
    }

    public void setContainerPort(Integer containerPort) {
        this.containerPort = containerPort;
    }

    public Integer getHostPort() {
        return hostPort;
    }

    public void setHostPort(Integer hostPort) {
        this.hostPort = hostPort;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
}
