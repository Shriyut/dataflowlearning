package org.learning.beam.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamingWordCount {
    public static void main(String[] args) throws IOException, URISyntaxException {
        ClassLoader loader = StreamingWordCount.class.getClassLoader();
        URI uri = loader.getResource("lorem.txt").toURI();
        Path path = Paths.get(uri);
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

        Pipeline pipeline = Pipeline.create();
        // unbounded data source
        TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());

        Instant now = Instant.now();

        // add all lines with timestamps to the TestStream
        List<TimestampedValue<String>> timestamped =
                IntStream.range(0, lines.size())
                        .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
                        .collect(Collectors.toList());

        for (TimestampedValue<String> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }

        // create the unbounded PCollection from TestStream
        PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());

        PCollection<String> words = input.apply(Tokenize.of());
        PCollection<String> windowed =
                words.apply(
                        Window.<String>into(new GlobalWindows())
                                .discardingFiredPanes()
                                .triggering(AfterWatermark.pastEndOfWindow()));

        windowed.apply(Count.perElement()).apply(PrintElements.of());

        pipeline.run().waitUntilFinish();


    }
}
