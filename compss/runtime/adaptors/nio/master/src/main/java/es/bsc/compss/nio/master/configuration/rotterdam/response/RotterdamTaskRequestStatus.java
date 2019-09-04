package es.bsc.compss.nio.master.configuration.rotterdam.response;

public class RotterdamTaskRequestStatus {

    private String resp;
    private String method;
    private String message;
    private String caasversion;
    private RotterdamTaskCheckDefinition task;


    public String getResp() {
        return resp;
    }

    public void setResp(String resp) {
        this.resp = resp;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getCaasversion() {
        return caasversion;
    }

    public void setCaasversion(String caasversion) {
        this.caasversion = caasversion;
    }

    public RotterdamTaskCheckDefinition getTask() {
        return task;
    }

    public void setTask(RotterdamTaskCheckDefinition task) {
        this.task = task;
    }
}
