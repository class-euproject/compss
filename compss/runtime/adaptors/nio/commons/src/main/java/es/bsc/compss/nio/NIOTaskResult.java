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
package es.bsc.compss.nio;

import es.bsc.compss.NIOProfile;
import es.bsc.compss.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;
import storage.StubItf;


public class NIOTaskResult implements Externalizable {

    private int jobId;

    private List<DataType> paramTypes = new LinkedList<>();
    // ATTENTION: Parameter Values will be empty if it doesn't contain a PSCO Id
    private List<Object> paramValues = new LinkedList<>();

    private NIOProfile p;


    /**
     * Only for externalization.
     */
    public NIOTaskResult() {
        // All attributes are initialized statically
    }

    /**
     * New task result with the given information.
     *
     * @param jobId Job Id.
     * @param arguments Job arguments.
     * @param target Job target.
     * @param results Job results.
     */
    public NIOTaskResult(int jobId, List<NIOParam> arguments, NIOParam target, List<NIOParam> results) {
        this.jobId = jobId;

        for (NIOParam np : arguments) {
            this.paramTypes.add(np.getType());

            if (np.isWriteFinalValue()) {
                // Object has direction INOUT or OUT
                switch (np.getType()) {
                    case PSCO_T:
                        this.paramValues.add(((StubItf) np.getValue()).getID());
                        break;
                    case EXTERNAL_PSCO_T:
                        this.paramValues.add(np.getValue());
                        break;
                    default:
                        // We add a NULL for any other type
                        this.paramValues.add(null);
                        break;
                }
            } else {
                // Object has direction IN
                this.paramValues.add(null);
            }
        }
        if (target != null) {
            this.paramTypes.add(target.getType());

            if (target.isWriteFinalValue()) {
                // Target is marked with isModifier = true
                switch (target.getType()) {
                    case PSCO_T:
                        this.paramValues.add(((StubItf) target.getValue()).getID());
                        break;
                    case EXTERNAL_PSCO_T:
                        this.paramValues.add(target.getValue());
                        break;
                    default:
                        // We add a NULL for any other type
                        this.paramValues.add(null);
                        break;
                }
            } else {
                // Target is marked with isModifier = false
                this.paramValues.add(null);
            }
        }

        for (NIOParam np : results) {
            this.paramTypes.add(np.getType());

            switch (np.getType()) {
                case PSCO_T:
                    this.paramValues.add(((StubItf) np.getValue()).getID());
                    break;
                case EXTERNAL_PSCO_T:
                    this.paramValues.add(np.getValue());
                    break;
                default:
                    // We add a NULL for any other type
                    this.paramValues.add(null);
                    break;
            }
        }
        this.p = null;
    }

    /**
     * New task result with the given information.
     *
     * @param jobId Job Id.
     * @param arguments Job arguments.
     * @param target Job target.
     * @param results Job results.
     * @param p NIOProfile containing timings of stard/end of task execution
     */
    public NIOTaskResult(int jobId, List<NIOParam> arguments, NIOParam target, List<NIOParam> results, NIOProfile p) {
        this.jobId = jobId;

        for (NIOParam np : arguments) {
            this.paramTypes.add(np.getType());

            if (np.isWriteFinalValue()) {
                // Object has direction INOUT or OUT
                switch (np.getType()) {
                    case PSCO_T:
                        this.paramValues.add(((StubItf) np.getValue()).getID());
                        break;
                    case EXTERNAL_PSCO_T:
                        this.paramValues.add(np.getValue());
                        break;
                    default:
                        // We add a NULL for any other type
                        this.paramValues.add(null);
                        break;
                }
            } else {
                // Object has direction IN
                this.paramValues.add(null);
            }
        }
        if (target != null) {
            this.paramTypes.add(target.getType());

            if (target.isWriteFinalValue()) {
                // Target is marked with isModifier = true
                switch (target.getType()) {
                    case PSCO_T:
                        this.paramValues.add(((StubItf) target.getValue()).getID());
                        break;
                    case EXTERNAL_PSCO_T:
                        this.paramValues.add(target.getValue());
                        break;
                    default:
                        // We add a NULL for any other type
                        this.paramValues.add(null);
                        break;
                }
            } else {
                // Target is marked with isModifier = false
                this.paramValues.add(null);
            }
        }

        for (NIOParam np : results) {
            this.paramTypes.add(np.getType());

            switch (np.getType()) {
                case PSCO_T:
                    this.paramValues.add(((StubItf) np.getValue()).getID());
                    break;
                case EXTERNAL_PSCO_T:
                    this.paramValues.add(np.getValue());
                    break;
                default:
                    // We add a NULL for any other type
                    this.paramValues.add(null);
                    break;
            }
        }

        this.p = p;
    }

    /**
     * Returns the job id associated to the result.
     *
     * @return The job Id associated to the result.
     */
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the parameter types.
     *
     * @return The parameter types
     */
    public List<DataType> getParamTypes() {
        return this.paramTypes;
    }

    /**
     * Returns the value of the parameter {@code i}.
     *
     * @param i Parameter index.
     * @return The value of the parameter {@code i}.
     */
    public Object getParamValue(int i) {
        return this.paramValues.get(i);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.jobId = in.readInt();
        this.paramTypes = (LinkedList<DataType>) in.readObject();
        this.paramValues = (LinkedList<Object>) in.readObject();
        this.p = (NIOProfile) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.jobId);
        out.writeObject(this.paramTypes);
        out.writeObject(this.paramValues);
        out.writeObject(this.p);
    }

    public NIOProfile getProfile() {
        return this.p;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[JOB_RESULT ");
        sb.append("[JOB ID= ").append(this.jobId).append("]");
        sb.append("[PARAM_TYPES");
        for (DataType param : this.paramTypes) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("[PARAM_VALUES");
        for (Object param : this.paramValues) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

}
