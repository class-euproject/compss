/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.agent.rest;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.agent.Agent;
import es.bsc.compss.agent.AgentException;
import es.bsc.compss.agent.RESTAgentConstants;
import es.bsc.compss.agent.rest.types.ApplicationParameterImpl;
import es.bsc.compss.agent.rest.types.Orchestrator;
import es.bsc.compss.agent.rest.types.messages.EndApplicationNotification;
import es.bsc.compss.agent.rest.types.messages.StartApplicationRequest;
import es.bsc.compss.agent.rest.types.messages.IncreaseNodeNotification;
import es.bsc.compss.agent.rest.types.messages.ReduceNodeRequest;
import es.bsc.compss.agent.rest.types.messages.RemoveNodeRequest;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.agent.util.RemoteJobsRegistry;
import es.bsc.compss.types.annotations.parameter.DataType;

import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


@Path("/COMPSs")
public class RESTAgent {

    @GET
    @Path("test/")
    public Response test() {
        System.out.println("test invoked");
        return Response.ok().build();
    }

    @PUT
    @Path("addResources/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response addResource(IncreaseNodeNotification notification) {
        Resource r = notification.getResource();
        //Updating processors
        MethodResourceDescription description = r.getDescription();
        List<Processor> procs = description.getProcessors();
        description.setProcessors(procs);

        try {
            Agent.addResources(r);
        } catch (AgentException ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(ex.getMessage()).build();
        }
        return Response.ok().build();
    }

    @PUT
    @Path("removeResources/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response removeResources(ReduceNodeRequest request) {
        String name = request.getWorkerName();
        MethodResourceDescription mrd = request.getResources();
        List<Processor> procs = mrd.getProcessors();
        mrd.setProcessors(procs);
        try {
            Agent.removeResources(name, mrd);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

        }
        return Response.ok().build();
    }

    @PUT
    @Path("removeNode/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response removeResource(RemoveNodeRequest request) {
        String name = request.getWorkerName();
        try {
            Agent.removeNode(name);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

        }
        return Response.ok().build();
    }

    @PUT
    @Path("startApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startApplication(StartApplicationRequest request) {
        Response response;
        String ceiClass = request.getCeiClass();
        if (ceiClass != null) {
            response = runMain(request);
        } else {
            response = runTask(request);
        }
        return response;
    }

    private static Response runMain(StartApplicationRequest request) {
        String serviceInstanceId = request.getServiceInstanceId();
        String ceiClass = request.getCeiClass();

        String className = request.getClassName();
        String methodName = request.getMethodName();
        Object[] params;
        try {
            params = request.getParamsValuesContent();
        } catch (Exception cnfe) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
                    "Could not recover an input parameter value. " + cnfe.getLocalizedMessage()
            ).build();
        }
        AppMainMonitor monitor = new AppMainMonitor();
        long appId;
        try {
            appId = Agent.runMain(Lang.JAVA, ceiClass, className, methodName, params, monitor);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.ok(appId, MediaType.TEXT_PLAIN).build();
    }

    private static Response runTask(StartApplicationRequest request) {
        String className = request.getClassName();
        String methodName = request.getMethodName();
        ApplicationParameterImpl[] sarParams = request.getParams();
        ApplicationParameterImpl target = request.getTarget();
        boolean hasResult = request.isHasResult();
        long appId;
        Orchestrator orchestrator = request.getOrchestrator();
        int numParams = sarParams.length;
        if (target != null) {
            numParams++;
        }
        if (hasResult) {
            numParams++;
        }
        AppTaskMonitor monitor = new AppTaskMonitor(numParams, orchestrator);

        try {
            appId = Agent.runTask(Lang.JAVA, className, methodName, sarParams, target, hasResult, monitor);
        } catch (AgentException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.ok(appId, MediaType.TEXT_PLAIN).build();
    }

    @PUT
    @Path("endApplication/")
    @Consumes(MediaType.APPLICATION_XML)
    public Response endApplication(EndApplicationNotification notification) {
        String jobId = notification.getJobId();
        JobEndStatus endStatus = notification.getEndStatus();
        DataType[] resultTypes = notification.getParamTypes();
        String[] resultLocations = notification.getParamLocations();
        RemoteJobsRegistry.notifyJobEnd(jobId, endStatus, resultTypes, resultLocations);

        return Response.ok().build();
    }

    public static void main(String[] args) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        int port = Integer.parseInt(args[0]);
        System.setProperty(RESTAgentConstants.COMPSS_AGENT_PORT, args[0]);
        Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", RESTAgent.class.getCanonicalName());

        try {
            jettyServer.start();
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }
}
