package es.bsc.compss.nio.master.starters;

import com.google.gson.Gson;

import com.google.gson.GsonBuilder;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.configuration.rotterdam.config.ContainerConfig;
import es.bsc.compss.nio.master.configuration.rotterdam.config.QosConfig;
import es.bsc.compss.nio.master.configuration.rotterdam.config.RotterdamTaskDefinition;
import es.bsc.compss.nio.master.configuration.rotterdam.response.RotterdamTaskCreateResponse;
import es.bsc.compss.nio.master.configuration.rotterdam.response.RotterdamTaskRequestStatus;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


public class RotterdamStarter extends ContainerStarter {

    private static final String TASK_CREATE_URL = "/api/v1/docks/tasks-compss";
    private static final String TASK_CHECK_TEMPLATE = "/api/v1/docks/%s/tasks/%s";
    private static final String SERVER_BASE = "http://rotterdam-caas.192.168.7.28.xip.io";

    private CloseableHttpClient httpClient;

    private String imageName;
    private int replicas;


    /**
     * Generic starter for workers running over containers.
     *
     * @param nw Worker node on which to run
     */
    public RotterdamStarter(NIOWorkerNode nw) {
        super(nw);
        this.imageName = nw.getConfiguration().getProperty("ImageName");
        this.httpClient = HttpClients.createDefault();
        this.replicas =
            Optional.ofNullable(nw.getConfiguration().getProperty("Replicas")).map(Integer::parseInt).orElse(1);
    }

    @Override
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        String dock = "class";
        String name = this.imageToContainerName(this.imageName) + "-" + DEPLOYMENT_ID.split("-")[0];
        RotterdamTaskDefinition createConfig = new RotterdamTaskDefinition(name, dock, replicas);

        this.nw.getConfiguration().getAdditionalProperties().entrySet().stream()
            .filter(e -> e.getKey().startsWith("qos:")).findFirst()
            .map(e -> new QosConfig(e.getKey().replaceAll("qos:", ""), e.getValue())).ifPresent(createConfig::setQos);

        ContainerConfig containerConfig = new ContainerConfig(name, this.imageName);
        containerConfig.setCommand(Arrays.asList("/bin/sh", "-c"));
        containerConfig.setArgs(
            Arrays.asList("mkdir -p " + this.nw.getWorkingDir() + "/jobs && " + "mkdir -p " + this.nw.getWorkingDir()
                + "/log && " + String.join(" ", generateStartCommand(43001, master, "NoTracingHostID"))));

        // IntStream.range(minPort, maxPort).forEach(n -> containerConfig.addPort(n, n, "tcp"));
        containerConfig.addPort(43001, minPort, "tcp");

        // Gson g = new Gson();
        Gson g = new GsonBuilder().setPrettyPrinting().create();
        try {
            createConfig.addContainer(containerConfig);
            System.out.println(g.toJson(createConfig));

            HttpPost request = new HttpPost(SERVER_BASE.concat(TASK_CREATE_URL));
            request.setEntity(new StringEntity(g.toJson(createConfig)));
            request.setHeader("Content-Type", "application/json");

            CloseableHttpResponse httpResponse = httpClient.execute(request);
            RotterdamTaskCreateResponse taskCreateResponse =
                g.fromJson(IOUtils.toString(httpResponse.getEntity().getContent()), RotterdamTaskCreateResponse.class);
            if (httpResponse.getStatusLine().getStatusCode() != 200 && "ok".equals(taskCreateResponse.getResp())) {
                throw new InitNodeException("Task could not be created");
            }
            httpResponse.close();

            RotterdamTaskRequestStatus status;
            do {
                HttpGet taskCheckRequest =
                    new HttpGet(SERVER_BASE.concat(String.format(TASK_CHECK_TEMPLATE, dock, name)));
                CloseableHttpResponse response = httpClient.execute(taskCheckRequest);
                String responseJson = IOUtils.toString(response.getEntity().getContent(), "utf-8");
                System.out.println(g.toJson(g.fromJson(responseJson, Map.class)));
                status = g.fromJson(responseJson, RotterdamTaskRequestStatus.class);
                if (response.getStatusLine().getStatusCode() == 200 && "ok".equals(status.getResp())
                    && !status.getTask().getPods().isEmpty()) {
                    break;
                }
                Thread.sleep(1000);
            } while (true);

            return new NIONode(status.getTask().getPods().get(0).getIp(), status.getTask().getPods().get(0).getPort());
        } catch (Exception e) {
            throw new InitNodeException(e);
        }
    }

    @Override
    protected String[] getStopCommand(int pid) {
        return new String[0];
    }
}
