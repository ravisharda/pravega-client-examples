package org.example.pravega.client.driver.utilities;

import lombok.NonNull;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

    public static String absolutePathOfFileInClasspath (@NonNull String fileLocation) {
        Path filePath = null;
        try {
            filePath = Paths.get(
                    (new FileUtils()).getClass().getClassLoader().getResource(fileLocation).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return filePath.toAbsolutePath().toString();
    }
}
