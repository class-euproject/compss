package es.bsc.compss.scheduler.rtheuristics;

import es.bsc.compss.COMPSsConstants;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class RTHeuristicsConfiguration {

    public static String INPUT_FILE = "/tmp/input.txt";


    public static void load() {
        String configFile = System.getProperty(COMPSsConstants.SCHEDULER_CONFIG_FILE);
        if (configFile != null && !configFile.isEmpty()) {
            try {
                PropertiesConfiguration conf = new PropertiesConfiguration(configFile);
                INPUT_FILE = conf.getString("paper.inputfile");
            } catch (ConfigurationException e) {
                // Do nothing. Res de res
            }
        }
    }

}