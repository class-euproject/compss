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
package es.bsc.compss.nio.master.utils;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOParamCollection;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Build NIOParam from other data types.
 * 
 * @see NIOParam
 */
public class NIOParamFactory {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Construct a NIOParam from a Parameter object. Necessary to translate master representations of parameters to
     * something transferable.
     * 
     * @param param Parameter.
     * @param node NIO Worker node
     * @return NIOParam representing this Parameter.
     */
    public static NIOParam fromParameter(Parameter param, NIOWorkerNode node) {
        NIOParam np;
        switch (param.getType()) {
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
                np = buildNioDependencyParam(param, node);
                break;
            case COLLECTION_T:
                NIOParam collNioParam = buildNioDependencyParam(param, node);
                np = buildNioCollectionParam(param, collNioParam, node);
                break;
            default:
                np = buildNioBasicParam(param);
                break;
        }

        return np;
    }

    private static NIOParam buildNioDependencyParam(Parameter param, NIOWorkerNode node) {
        DependencyParameter dPar = (DependencyParameter) param;
        Object value = dPar.getDataTarget();

        // Check if the parameter has a valid PSCO and change its type
        // OUT objects are restricted by the API
        String renaming = null;
        String dataMgmtId;
        DataAccessId dAccId = dPar.getDataAccessId();
        if (dAccId instanceof RWAccessId) {
            // Read write mode
            RWAccessId rwaId = (RWAccessId) dAccId;
            renaming = rwaId.getReadDataInstance().getRenaming();
            dataMgmtId = rwaId.getWrittenDataInstance().getRenaming();
        } else if (dAccId instanceof RAccessId) {
            // Read only mode
            RAccessId raId = (RAccessId) dAccId;
            renaming = raId.getReadDataInstance().getRenaming();
            dataMgmtId = renaming;
        } else {
            WAccessId waId = (WAccessId) dAccId;
            dataMgmtId = waId.getWrittenDataInstance().getRenaming();
        }
        if (renaming != null) {
            String pscoId = Comm.getData(renaming).getPscoId();
            if (pscoId != null) {
                if (param.getType().equals(DataType.OBJECT_T)) {
                    // Change Object type if it is a PSCO
                    param.setType(DataType.PSCO_T);
                } else if (param.getType().equals(DataType.FILE_T)) {
                    // Change external object type for Python PSCO return objects
                    param.setType(DataType.EXTERNAL_PSCO_T);
                }
            }
        }

        /*
         * Fix for the is replicated tasks with inout/out parameters. We have to generate output data target according
         * to the node
         */
        if ((dAccId instanceof RWAccessId) || (dAccId instanceof WAccessId)) {
            if (!param.getType().equals(DataType.PSCO_T) && !param.getType().equals(DataType.EXTERNAL_PSCO_T)) {
                value = node.getOutputDataTarget(dataMgmtId, dPar);
            }
        }

        // Create the NIO Param
        boolean writeFinalValue = !(dAccId instanceof RAccessId); // Only store W and RW
        NIOParam np = new NIOParam(dataMgmtId, param.getType(), param.getStream(), param.getPrefix(), param.getName(),
            param.getContentType(), preserveSourceData, writeFinalValue, value, (NIOData) dPar.getDataSource(),
            dPar.getOriginalName());
        return np;
    }

    private static NIOParam buildNioCollectionParam(Parameter param, NIOParam collNioParam, NIOWorkerNode node) {
        if (DEBUG) {
            LOGGER.debug("Detected COLLECTION_T parameter");
        }

        NIOParamCollection npc = new NIOParamCollection(collNioParam);

        CollectionParameter collParam = (CollectionParameter) param;
        for (Parameter subParam : collParam.getParameters()) {
            npc.addParameter(NIOParamFactory.fromParameter(subParam, node));
        }

        if (DEBUG) {
            LOGGER.debug(
                "NIOParamCollection with id = " + npc.getDataMgmtId() + " contains " + npc.getSize() + " parameters.");
        }

        return npc;
    }

    private static NIOParam buildNioBasicParam(Parameter param) {
        BasicTypeParameter btParB = (BasicTypeParameter) param;
        Object value = btParB.getValue();
        boolean preserveSourceData = false; // Basic parameters are not preserved on Worker
        boolean writeFinalValue = false; // Basic parameters are not stored on Worker
        NIOParam np = new NIOParam(null, param.getType(), param.getStream(), param.getPrefix(), param.getName(),
            param.getContentType(), preserveSourceData, writeFinalValue, value, null, DependencyParameter.NO_NAME);
        return np;
    }
}
