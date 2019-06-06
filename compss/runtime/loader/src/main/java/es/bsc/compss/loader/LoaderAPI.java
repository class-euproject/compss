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
package es.bsc.compss.loader;

import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.parameter.Direction;


public interface LoaderAPI {

    /**
     * Returns the renaming of the file version opened.
     * 
     * @param fileName File.
     * @param mode Access mode.
     * @return Renaming of the current file version.
     */
    public String openFile(String fileName, Direction mode);

    /**
     * Closes the given file {@code fileName}.
     * 
     * @param fileName File version name.
     * @param mode Access mode.
     */
    public void closeFile(String fileName, Direction mode);

    /**
     * Deletes the specified version of a file.
     * 
     * @param fileName File version name.
     * @return {@code true} if the file has been erased, {@code false} otherwise.
     */
    public boolean deleteFile(String fileName);

    /**
     * Retrieves the last version of file with its original name.
     * 
     * @param appId Application id.
     * @param fileName File name.
     */
    public void getFile(Long appId, String fileName);

    /**
     * Returns a copy of the last version of the given object {@code o}.
     * 
     * @param o Object.
     * @param hashCode Object hashcode.
     * @param destDir Destination directory for serialization.
     * @return In-memory copy of the last version of the given object.
     */
    public Object getObject(Object o, int hashCode, String destDir);

    /**
     * Serializes the given object {@code o} to the given path {@code destDir}.
     * 
     * @param o Object.
     * @param hashCode Object hashcode.
     * @param destDir Destination directory.
     */
    public void serializeObject(Object o, int hashCode, String destDir);

    /**
     * Returns the Object Registry instance.
     * 
     * @return The Object Registry instance.
     */
    public ObjectRegistry getObjectRegistry();

    /**
     * Returns the Stream Registry instance.
     * 
     * @return The Stream Registry instance.
     */
    public StreamRegistry getStreamRegistry();

    /**
     * Associates a new Object Registry.
     * 
     * @param oReg Object Registry.
     */
    public void setObjectRegistry(ObjectRegistry oReg);

    /**
     * Associates a new Stream Registry instance.
     * 
     * @param sReg Stream Registry.
     */
    public void setStreamRegistry(StreamRegistry sReg);

    /**
     * Returns the directory where to store temporary files.
     * 
     * @return The directory where to store temporary files.
     */
    public String getTempDir();

    /**
     * Removes the given object {@code o}.
     * 
     * @param o Object.
     * @param hashcode Object hashcode.
     */
    public void removeObject(Object o, int hashcode);

}
