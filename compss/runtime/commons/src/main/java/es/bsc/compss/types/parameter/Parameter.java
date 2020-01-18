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
package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import java.io.Serializable;


public abstract class Parameter implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Parameter fields
    private DataType type;
    private final Direction direction;
    private final StdIOStream stream;
    private final String prefix;
    private final String name;
    private final String contentType;


    /**
     * Creates a new Parameter instance from the given values.
     * 
     * @param type Parameter type.
     * @param direction Parameter direction.
     * @param stream Parameter IO stream mode.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Python object type.
     */
    public Parameter(DataType type, Direction direction, StdIOStream stream, String prefix, String name,
        String contentType) {
        this.type = type;
        this.direction = direction;
        this.stream = stream;
        if (prefix == null || prefix.isEmpty()) {
            this.prefix = Constants.PREFIX_EMPTY;
        } else {
            this.prefix = prefix;
        }
        this.name = name;
        this.contentType = contentType;
    }

    /**
     * Returns the parameter type.
     * 
     * @return The parameter type.
     */
    public DataType getType() {
        return this.type;
    }

    /**
     * Sets a new parameter type.
     * 
     * @param type New parameter type.
     */
    public void setType(DataType type) {
        this.type = type;
    }

    /**
     * Returns the parameter direction.
     * 
     * @return The parameter direction.
     */
    public Direction getDirection() {
        return this.direction;
    }

    /**
     * Returns the parameter IO stream mode.
     * 
     * @return The parameter IO stream mode.
     */
    public StdIOStream getStream() {
        return this.stream;
    }

    /**
     * Returns the parameter prefix.
     * 
     * @return The parameter prefix.
     */
    public String getPrefix() {
        return this.prefix;
    }

    /**
     * Returns the parameter name.
     * 
     * @return The parameter name.
     */
    public String getName() {
        return this.name;
    }

    public String getContentType() {
        return this.contentType;
    }
}
