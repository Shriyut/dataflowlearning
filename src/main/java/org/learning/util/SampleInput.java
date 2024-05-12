package org.learning.util;

import org.learning.beam.basic.WordCount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SampleInput {

    public static List<String> getLines() throws IOException, URISyntaxException {
                ClassLoader loader =SampleInput.class.getClassLoader();
                URI uri = loader.getResource("lorem.txt").toURI();
                Path path = Paths.get(uri);
                //List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

                return lines;
    }
}
