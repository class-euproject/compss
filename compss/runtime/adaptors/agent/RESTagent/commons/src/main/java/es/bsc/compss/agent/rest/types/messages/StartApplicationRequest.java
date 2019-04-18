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
package es.bsc.compss.agent.rest.types.messages;

import es.bsc.compss.agent.rest.types.Orchestrator;
import es.bsc.compss.agent.rest.types.ApplicationParameterImpl;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ArrayParameter;
import es.bsc.compss.agent.rest.types.ApplicationParameterValue.ElementParameter;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.parameter.Parameter;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "startApplication")
public class StartApplicationRequest implements Serializable {

    /**
     * OBjects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private String serviceInstanceId;
    private String ceiClass;
    private String className;
    private String methodName;
    private ApplicationParameterImpl[] params = new ApplicationParameterImpl[0];
    private ApplicationParameterImpl target;
    private boolean hasResult;
    private Orchestrator orchestrator;


    public StartApplicationRequest() {

    }

    public void setServiceInstanceId(String serviceInstanceId) {
        this.serviceInstanceId = serviceInstanceId;
    }

    public String getServiceInstanceId() {
        return serviceInstanceId;
    }

    public String getCeiClass() {
        return ceiClass;
    }

    public void setCeiClass(String ceiClass) {
        this.ceiClass = ceiClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ApplicationParameterImpl getTarget() {
        return target;
    }

    public void setTarget(ApplicationParameterImpl target) {
        this.target = target;
    }

    public void addParameter(boolean value) {
        addParameter(value, Direction.IN, DataType.BOOLEAN_T);
    }

    public void addParameter(byte value) {
        addParameter(value, Direction.IN, DataType.BYTE_T);
    }

    public void addParameter(char value) {
        addParameter(value, Direction.IN, DataType.CHAR_T);
    }

    public void addParameter(short value) {
        addParameter(value, Direction.IN, DataType.SHORT_T);
    }

    public void addParameter(int value) {
        addParameter(value, Direction.IN, DataType.INT_T);
    }

    public void addParameter(long value) {
        addParameter(value, Direction.IN, DataType.LONG_T);
    }

    public void addParameter(float value) {
        addParameter(value, Direction.IN, DataType.FLOAT_T);
    }

    public void addParameter(double value) {
        addParameter(value, Direction.IN, DataType.DOUBLE_T);
    }

    public void addParameter(String value) {
        addParameter(value, Direction.IN, DataType.STRING_T);
    }

    public void addParameter(Object value) {
        addParameter(value, Direction.IN);
    }

    public void addParameter(Object value, Direction direction) {
        addParameter(value, Direction.IN, DataType.OBJECT_T);
    }

    public void addPersistedParameter(String id) {
        addPersistedParameter(id, Direction.IN);
    }

    public void addPersistedParameter(String id, Direction direction) {
        ApplicationParameterImpl p = addParameter(id, direction, DataType.PSCO_T);
        ((ElementParameter) p.getValue()).setClassName("storage.StubItf");
    }

    public void addParameter(Parameter p, Object value) {
        addParameter(value, p.getDirection(), p.getType());
    }

    private ApplicationParameterImpl addParameter(Object value, Direction direction, DataType type) {
        ApplicationParameterImpl p = new ApplicationParameterImpl(value, direction, type, Stream.UNSPECIFIED);
        p.setParamId(params.length);

        ApplicationParameterImpl[] oldParams = params;
        params = new ApplicationParameterImpl[oldParams.length + 1];
        if (oldParams.length > 0) {
            System.arraycopy(oldParams, 0, params, 0, oldParams.length);
        }
        params[oldParams.length] = p;
        return p;
    }

    @XmlElementWrapper(name = "parameters")
    public ApplicationParameterImpl[] getParams() {
        return params;
    }

    public void setParams(ApplicationParameterImpl[] params) {
        this.params = params;
    }

    public ApplicationParameterValue[] getParamsValues() throws ClassNotFoundException {
        int paramsCount = params.length;
        ApplicationParameterValue[] paramValues = new ApplicationParameterValue[paramsCount];
        for (ApplicationParameterImpl param : params) {
            int paramIdx = param.getParamId();
            paramValues[paramIdx] = param.getValue();
        }
        return paramValues;
    }

    public Object[] getParamsValuesContent() throws Exception {
        int paramsCount = params.length;
        Object[] paramValues = new Object[paramsCount];
        for (ApplicationParameterImpl param : params) {
            int paramIdx = param.getParamId();
            paramValues[paramIdx] = param.getValue().getContent();
        }
        return paramValues;
    }

    public void setHasResult(boolean hasResult) {
        this.hasResult = hasResult;
    }

    public boolean isHasResult() {
        return hasResult;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StartApplication ").append(className).append(".").append(methodName).append("(");

        int count = 0;
        for (ApplicationParameterImpl param : this.params) {
            if (count > 0) {
                sb.append(", ");
            }
            count++;
            if (param.getValue() instanceof ArrayParameter) {
                sb.append(param.getType());
            } else {
                sb.append(param.getType());
            }
        }
        sb.append(") defined in CEI ").append(ceiClass);
        return sb.toString();
    }

    public void setOrchestrator(String host, Orchestrator.HttpMethod method, String operation) {
        this.orchestrator = new Orchestrator(host, method, operation);
    }

    public void setOrchestrator(Orchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public Orchestrator getOrchestrator() {
        return orchestrator;
    }

}
