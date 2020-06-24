package org.example.pravega.shared;

import com.google.common.io.Resources;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Paths;

@Slf4j
class PathUtils {

    @SneakyThrows
    public static String locationFromClasspath(@NonNull String fileLocation) {
        URL url = Resources.getResource(fileLocation);
        if (url == null) {
            return null;
        } else {
            return Paths.get(url.toURI()).toAbsolutePath().toString();
        }
    }
}
