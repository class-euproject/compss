package es.bsc.compss.nio.master.configuration.rotterdam.response;

import es.bsc.compss.nio.master.configuration.rotterdam.config.RotterdamTaskDefinition;


public class RotterdamTaskCreateResponseTask {

    private String dbid;
    private String id;
    private String name;
    private String nameSpace;
    private String type;
    private String status;
    private String agreementId;
    private Integer replicas;
    private RotterdamTaskDefinition taskDefinition;
    private String clusterId;


    public RotterdamTaskCreateResponseTask(String name, Integer replicas) {
        this.name = name;
        this.replicas = replicas;
    }

    public String getDbid() {
        return dbid;
    }

    public void setDbid(String dbid) {
        this.dbid = dbid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAgreementId() {
        return agreementId;
    }

    public void setAgreementId(String agreementId) {
        this.agreementId = agreementId;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public RotterdamTaskDefinition getTaskDefinition() {
        return taskDefinition;
    }

    public void setTaskDefinition(RotterdamTaskDefinition taskDefinition) {
        this.taskDefinition = taskDefinition;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
