/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.resources;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;


public class MasterResourceImpl extends DynamicMethodWorker implements MasterResource {

    private static final String MASTER_NAME_PROPERTY = System.getProperty(COMPSsConstants.MASTER_NAME);
    private static final String UNDEFINED_MASTER_NAME = "master";

    protected static final String ERROR_COMPSs_LOG_BASE_DIR = "ERROR: Cannot create .COMPSs base log directory";
    protected static final String ERROR_APP_OVERLOAD = "ERROR: Cannot erase overloaded directory";
    protected static final String ERROR_APP_LOG_DIR = "ERROR: Cannot create application log directory";
    protected static final String ERROR_TEMP_DIR = "ERROR: Cannot create temp directory";
    protected static final String ERROR_JOBS_DIR = "ERROR: Cannot create jobs directory";
    protected static final String ERROR_WORKERS_DIR = "ERROR: Cannot create workers directory";
    protected static final String WARN_FOLDER_OVERLOAD = "WARNING: Reached maximum number of executions for this application. To avoid this warning please clean .COMPSs folder";

    protected static final int MAX_OVERLOAD = 100; // Maximum number of executions of same application

    private final String userExecutionDirPath;

    private final String COMPSsLogBaseDirPath;
    private final String appLogDirPath;

    private final String tempDirPath;
    private final String jobsDirPath;
    private final String workersDirPath;

    public static final String MASTER_NAME;

    static {
        // Initializing host attributes
        String hostName = "";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals("")) && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            hostName = MASTER_NAME_PROPERTY;
        } else {
            // The MASTER_NAME_PROPERTY has not been defined, try load from machine
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                hostName = localHost.getCanonicalHostName();
            } catch (UnknownHostException e) {
                // Sets a default hsotName value
                ErrorManager.warn("ERROR_UNKNOWN_HOST: " + e.getLocalizedMessage());
                hostName = UNDEFINED_MASTER_NAME;
            }
        }
        MASTER_NAME = hostName;
    }

    public MasterResourceImpl() {
        super(MASTER_NAME, new MethodResourceDescription(), new COMPSsMaster(MASTER_NAME), 0, 0, 0, 0, new HashMap<String, String>());

        // Gets user execution directory
        userExecutionDirPath = System.getProperty("user.dir");

        /* Creates base Runtime structure directories ************************** */
        boolean mustCreateExecutionSandbox = true;
        // Checks if specific log base dir has been given
        String specificOpt = System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR);
        if (specificOpt != null && !specificOpt.isEmpty()) {
            COMPSsLogBaseDirPath = specificOpt.endsWith(File.separator) ? specificOpt : specificOpt + File.separator;
            mustCreateExecutionSandbox = false; // This is the only case where
            // the sandbox is provided
        } else {
            // Checks if base log dir has been given
            String baseOpt = System.getProperty(COMPSsConstants.BASE_LOG_DIR);
            if (baseOpt != null && !baseOpt.isEmpty()) {
                baseOpt = baseOpt.endsWith(File.separator) ? baseOpt : baseOpt + File.separator;
                COMPSsLogBaseDirPath = baseOpt + ".COMPSs" + File.separator;
            } else {
                // No option given - load default (user home)
                COMPSsLogBaseDirPath = System.getProperty("user.home") + File.separator + ".COMPSs" + File.separator;
            }
        }

        if (!new File(COMPSsLogBaseDirPath).exists()) {
            if (!new File(COMPSsLogBaseDirPath).mkdir()) {
                ErrorManager.error(ERROR_COMPSs_LOG_BASE_DIR);
            }
        }

        // Load working directory. Different for regular applications and
        // services
        if (mustCreateExecutionSandbox) {
            String appName = System.getProperty(COMPSsConstants.APP_NAME);
            if (System.getProperty(COMPSsConstants.SERVICE_NAME) != null) {
                /*
                 * SERVICE - Gets appName - Overloads the service folder for different executions - MAX_OVERLOAD raises
                 * warning - Changes working directory to serviceName !!!!
                 */
                String serviceName = System.getProperty(COMPSsConstants.SERVICE_NAME);
                int overloadCode = 1;
                String appLog = COMPSsLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode) + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog = COMPSsLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode) + File.separator;
                    } else {
                        appLog = COMPSsLogBaseDirPath + serviceName + "_" + String.valueOf(overloadCode) + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                appLogDirPath = appLog;
                if (!new File(appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            } else {
                /*
                 * REGULAR APPLICATION - Gets appName - Overloads the app folder for different executions - MAX_OVERLOAD
                 * raises warning - Changes working directory to appName !!!!
                 */
                int overloadCode = 1;
                String appLog = COMPSsLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog = COMPSsLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                    } else {
                        appLog = COMPSsLogBaseDirPath + appName + "_" + String.valueOf(overloadCode) + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                appLogDirPath = appLog;
                if (!new File(appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            }
        } else {
            // The option specific_log_dir has been given. NO sandbox created
            appLogDirPath = COMPSsLogBaseDirPath;
        }

        // Set the environment property (for all cases) and reload logger
        // configuration
        System.setProperty(COMPSsConstants.APP_LOG_DIR, appLogDirPath);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();

        /*
         * Create a tmp directory where to store: - Files whose first opened stream is an input one - Object files
         */
        tempDirPath = appLogDirPath + "tmpFiles" + File.separator;
        if (!new File(tempDirPath).mkdir()) {
            ErrorManager.error(ERROR_TEMP_DIR);
        }

        /*
         * Create a jobs dir where to store: - Jobs output files - Jobs error files
         */
        jobsDirPath = appLogDirPath + "jobs" + File.separator;
        if (!new File(jobsDirPath).mkdir()) {
            ErrorManager.error(ERROR_JOBS_DIR);
        }

        /*
         * Create a workers dir where to store: - Worker out files - Worker error files
         */
        workersDirPath = appLogDirPath + "workers" + File.separator;
        if (!new File(workersDirPath).mkdir()) {
            System.err.println(ERROR_WORKERS_DIR);
            System.exit(1);
        }
    }

    private boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return false;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else if (!f.delete()) {
                    return false;
                }
            }
        }

        return directory.delete();
    }

    public String getCOMPSsLogBaseDirPath() {
        return COMPSsLogBaseDirPath;
    }

    @Override
    public String getWorkingDirectory() {
        return tempDirPath;
    }

    public String getUserExecutionDirPath() {
        return userExecutionDirPath;
    }

    @Override
    public String getAppLogDirPath() {
        return appLogDirPath;
    }

    @Override
    public String getTempDirPath() {
        return tempDirPath;
    }

    @Override
    public String getJobsDirPath() {
        return jobsDirPath;
    }

    @Override
    public String getWorkersDirPath() {
        return workersDirPath;
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public Type getType() {
        return Type.MASTER;
    }

    @Override
    public int compareTo(Resource t) {
        if (t.getType() == Type.MASTER) {
            return getName().compareTo(t.getName());
        } else {
            return 1;
        }
    }

    @Override
    public void updateResource(MethodResourceDescription mrd, Map<String, String> sharedDisks) {
        this.description.increase(mrd);
        this.available.increase(mrd);
        this.setMaxCPUTaskCount(mrd.getTotalCPUComputingUnits());
        this.setMaxGPUTaskCount(mrd.getTotalGPUComputingUnits());
        this.setMaxFPGATaskCount(mrd.getTotalFPGAComputingUnits());
        this.setMaxOthersTaskCount(mrd.getTotalOTHERComputingUnits());
        super.sharedDisks = sharedDisks;
    }

}
