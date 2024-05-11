package org.learning.beam.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.learning.util.PrintElements;
import org.learning.util.Tokenize;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class WordCount {

    public static void main(String[] args) throws IOException, URISyntaxException {
        //ClassLoader loader = DemoClass.class.getClassLoader();
        ClassLoader loader = WordCount.class.getClassLoader();
        //String file = loader.getResource("lorem.txt").getFile();
        //String file = String.valueOf(loader.getResource("lorem.txt").toURI());
        URI uri = loader.getResource("lorem.txt").toURI();
        Path path = Paths.get(uri);
        //List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply(Create.of(lines));
        PCollection<String> words = input.apply(Tokenize.of());
        PCollection<KV<String, Long>> result = words.apply(Count.perElement());

        result.apply(PrintElements.of());
        pipeline.run().waitUntilFinish();

    }
}
