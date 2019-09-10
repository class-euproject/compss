package es.bsc.compss.test;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ContainerTest {

    private final String COMPSS_HOME = "/opt/COMPSs";
    private final String RUNCOMPSS = COMPSS_HOME + "/Runtime/scripts/user/runcompss";
    private final String BASE_PCK = "es.bsc.compss.apps.%s";

    public @Rule Timeout timeout = new Timeout(30000);

    private List<String> getOutput(InputStream input) {
        List<String> inputList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            String line;
            while ((line = reader.readLine()) != null) {
                inputList.add(line);
            }
        } catch (IOException e) {}
        return inputList;
    }

    @Test
    public void simpleDocker() throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder(RUNCOMPSS,
                "-d",
                "--project=simple-docker/project.xml",
                "--resources=simple-docker/resources.xml",
                String.format(BASE_PCK, "simple.Simple"),
                "5");
        Process process = processBuilder.inheritIO().start();
        assertEquals(0, process.waitFor());
        getOutput(process.getInputStream());
    }

    /*@Test
    public void simpleLXC() throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder(RUNCOMPSS,
                "-d",
                "--project=simple-lxc/project.xml",
                "--resources=simple-lxc/resources.xml",
                // "--master_name=192.168.0.106",
                String.format(BASE_PCK, "simple.Simple"),
                "5");
        Process process = processBuilder.inheritIO().start();
        assertEquals(0, process.waitFor());
        getOutput(process.getInputStream());
    }*/

}
