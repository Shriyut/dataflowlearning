package org.learning.beam.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
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

public class ProcessingTimeWindow {
    public static void main(String[] args) throws IOException, URISyntaxException {
        // no throws in main, use exception handling similarly for dofn but not for methods
        ClassLoader loader = ProcessingTimeWindow.class.getClassLoader();
        URI uri = loader.getResource("lorem.txt").toURI();
        Path path = Paths.get(uri);
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);


        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply(Create.of(lines));
        PCollection<String> words =
                input.apply(Tokenize.of()).apply(WithReadDelay.ofProcessingTime(Duration.millis(20)));

        // Window into single window and specify trigger
        PCollection<String> windowed =
                words.apply(
                        Window.<String>into(new GlobalWindows())
                                .triggering(
                                        Repeatedly.forever(
                                                AfterProcessingTime.pastFirstElementInPane()
                                                        .plusDelayOf(Duration.standardSeconds(1))))
//                                .discardingFiredPanes()); //99 124
                                .accumulatingFiredPanes()); //99 223
        // trying with accumulatingFiredPanes

        PCollection<Long> result = windowed.apply(Count.globally()); //runs on single worker
        result.apply(PrintElements.of());
        pipeline.run().waitUntilFinish();


    }
}
