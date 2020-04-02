package es.bsc.compss.scheduler.prometheus;

import es.bsc.compss.COMPSsConstants;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class PrometheusConfiguration {

    public static String PROMETHEUS_ENDPOINT = "localhost:9090";

    public static String PROMETHEUS_FILE = "/tmp/input.txt";


    public static void load() {
        String configFile = System.getProperty(COMPSsConstants.SCHEDULER_CONFIG_FILE);
        if (configFile != null && !configFile.isEmpty()) {
            try {
                PropertiesConfiguration conf = new PropertiesConfiguration(configFile);
                PROMETHEUS_ENDPOINT = conf.getString("prometheus.endpoint");
                PROMETHEUS_FILE = conf.getString("prometheus.inputfile");
            } catch (ConfigurationException e) {
                // Do nothing. Res de res
            }
        }
    }

}