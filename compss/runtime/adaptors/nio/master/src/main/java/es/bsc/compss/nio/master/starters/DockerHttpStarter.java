package es.bsc.compss.nio.master.starters;

import com.google.gson.Gson;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.starters.json.DockerContainerJsonState;
import es.bsc.compss.nio.master.starters.json.DockerCreateContainerRequest;
import es.bsc.compss.nio.master.starters.json.PortSettings;
import es.bsc.compss.util.Tracer;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class DockerHttpStarter extends ContainerStarter {

    private final String containerName;
    private final String imageName;


    /**
     * Starts a node on a Docker container.
     * 
     * @param node The node
     * @throws InitNodeException When an error occurs when initializing the node
     */
    public DockerHttpStarter(NIOWorkerNode node) throws InitNodeException {
        super(node);
        LOGGER.info("Using Docker HTTP starter");
        this.imageName = node.getConfiguration().getProperty("ImageName");
        this.containerName = this.imageToContainerName(this.imageName) + "-" + DEPLOYMENT_ID.split("-")[0];

        if (this.imageName == null) {
            throw new InitNodeException("Image name must be provided");
        }
    }

    @Override
    protected String[] getStopCommand(int pid) {
        return new String[0];
    }

    @Override
    protected NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException {
        String tracingHostId = "NoTracingHostID";
        if (Tracer.extraeEnabled()) {
            tracingHostId = String.valueOf(NIOTracer.registerHost(this.nw.getName(), 0));
        }
        Gson g = new Gson();
        String server = "http://" + this.nw.getName() + ":2376";
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            createContainer(server, client, g, generateStartCommand(43001, master, tracingHostId));
            startWorker(server, client, g);
            int port = getStateAndPortRedirection(server, client, g);
            return new NIONode(this.nw.getName(), port);
        } catch (IOException e) {
            e.printStackTrace();
            throw new InitNodeException(e);
        }
    }

    private void createContainer(String server, HttpClient client, Gson g, String[] command)
        throws IOException, InitNodeException {
        DockerCreateContainerRequest requestBody = new DockerCreateContainerRequest(command, this.imageName);
        requestBody.addPort(43001);
        HttpPost creationRequest = new HttpPost(server.concat("/containers/create?name=" + this.containerName));
        creationRequest.setEntity(new StringEntity(g.toJson(requestBody)));
        creationRequest.setHeader("Content-Type", "application/json");
        HttpResponse creationResponse = client.execute(creationRequest);
        Map<String, Object> creationResponseBody =
            g.fromJson(EntityUtils.toString(creationResponse.getEntity()), Map.class);
        if (creationResponse.getStatusLine().getStatusCode() != 201) {
            throw new InitNodeException(creationResponseBody.get("message").toString());
        }
    }

    private void startWorker(String server, HttpClient client, Gson g) throws IOException, InitNodeException {
        HttpPost startRequest = new HttpPost(server.concat("/containers/" + this.containerName + "/start"));
        HttpResponse startResponse = client.execute(startRequest);
        if (startResponse.getStatusLine().getStatusCode() != 204) {
            throw new InitNodeException("The container was created, but could not be started");
        }
    }

    private int getStateAndPortRedirection(String server, HttpClient client, Gson g)
        throws IOException, InitNodeException {
        HttpGet getRequest = new HttpGet(server.concat("/containers/" + this.containerName + "/json"));
        HttpResponse getResponse = client.execute(getRequest);
        DockerContainerJsonState containerState =
            g.fromJson(EntityUtils.toString(getResponse.getEntity()), DockerContainerJsonState.class);
        if (getResponse.getStatusLine().getStatusCode() != 200) {
            throw new InitNodeException(containerState.getMessage());
        }
        List<PortSettings> ports = containerState.getNetworkSettings().getPorts().get("43001/tcp");
        if (ports == null || ports.isEmpty()) {
            throw new InitNodeException("The port redirection could not be retrieved");
        }
        return Integer.parseInt(ports.get(0).getHostPort());
    }

}
