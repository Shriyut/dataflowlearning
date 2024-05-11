package org.learning.util;

//import com.google.common.base.Strings;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class Utils {

    public static List<String> getLines(InputStream stream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    public static List<String> toWords(String input) {
        return Arrays.stream(input.split("\\W+"))
                .filter(((Predicate<String>) Strings::isNullOrEmpty).negate())
                .collect(Collectors.toList());
    }

    public static List<String> readAllLines(InputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            return reader.lines().collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Utils() {}
}
