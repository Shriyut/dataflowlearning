package org.learning.beam.basic;


import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.Pipeline;

import java.io.IOException;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.learning.util.SampleInput;
import org.learning.util.Tokenize;


public class TestStreamClass {

    public static void main(String[] args) throws IOException, URISyntaxException {
        TestStream.Builder<String> streamBuilder =
                TestStream.create(StringUtf8Coder.of());
        List<String> lines = SampleInput.getLines();
        //fill the test stream with data
        Instant now = Instant.now();
        //add all lines with timestamps to the TestStream
        List<TimestampedValue<String>> timestamped =
                IntStream.range(0, lines.size())
                        .mapToObj(i -> TimestampedValue.of(
                                lines.get(i), now.plus(i)))
                        .collect(Collectors.toList());

        for(TimestampedValue<String> value : timestamped) {
            streamBuilder = streamBuilder.addElements(value);
        }

        Pipeline pipeline = Pipeline.create();
        //create unbounded PCollection from test stream
        PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
        PCollection<String> words = input.apply(Tokenize.of());
        words.apply(Count.perElement());

    }
}
