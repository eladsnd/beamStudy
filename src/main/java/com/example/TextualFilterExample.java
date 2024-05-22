package com.example;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextualFilterExample {

    private static final Logger LOG = LoggerFactory.getLogger(TextualFilterExample.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        String text = "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep";

        PCollection<String> input = pipeline.apply(Create.of(Arrays.asList(text.split(" "))));

        // Filter words that start with 'a' (case-insensitive)
        PCollection<String> wordsStartingWithA = StarsWithA(input);

        // Filter words with more than three characters
        PCollection<String> wordsMoreThanThreeChars = MoreThanThreeChars(input);

        // Combine both filters
        PCollection<String> combinedFilters = MoreThanThreeChars(StarsWithA(input));

//        wordsStartingWithA.apply("Log words starting with 'a'", ParDo.of(new LogOutput<>("Starts with 'a'")));
//        wordsMoreThanThreeChars.apply("Log words with more than 3 chars", ParDo.of(new LogOutput<>("More than 3 chars")));
        combinedFilters.apply("Log words starting with 'a' and more than 3 chars", ParDo.of(new LogOutput<>("Starts with 'a' and more than 3 chars")));

        pipeline.run().waitUntilFinish();
    }

    // The method transforms the collection to include only words starting with 'A' (case-insensitive)
    static PCollection<String> StarsWithA(PCollection<String> input) {
        return input.apply(Filter.by((String word) -> word.toUpperCase().startsWith("A")));
    }
    // The method transforms the collection to include only words with more than three characters
    static PCollection<String> MoreThanThreeChars(PCollection<String> input) {
        return input.apply(Filter.by((String word) -> word.length() > 3));
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
            c.output(c.element());
        }
    }
}
