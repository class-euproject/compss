package es.bsc.compss.scheduler.heuristics;

import es.bsc.compss.COMPSsConstants;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class HeuristicsConfiguration {

    public static String HEURISTICS_FILE = "/opt/COMPSs/Runtime/configuration/ilp/max_norm_16000_1_4";


    public static void load() {
        String configFile = System.getProperty(COMPSsConstants.SCHEDULER_CONFIG_FILE);
        if (configFile != null && !configFile.isEmpty()) {
            try {
                PropertiesConfiguration conf = new PropertiesConfiguration(configFile);
                HEURISTICS_FILE = conf.getString("heuristics.inputfile");
            } catch (ConfigurationException e) {
                // Do nothing. Res de res
            }
        }
    }

}