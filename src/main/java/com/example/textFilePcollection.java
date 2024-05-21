package com.example;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class textFilePcollection {

    private static final Logger LOG = LoggerFactory.getLogger(textFilePcollection.class);


    public static void main(String[] args) {
        String localFile = "C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\kinglear.txt";

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);

        // Concept #1. Read text file content line by line. resulting PCollection contains elements, where each element
        // contains a single line of text from the input file.
        PCollection<String> input = pipeline.apply(TextIO.read().from(localFile))
                .apply(Filter.by((String line) -> !line.isEmpty()));

        // Concept #2. Output first 10 elements PCollection to the console.
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);


        PCollection<String> sampleLines = input.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log lines", ParDo.of(new LogStrings()));


        //Concept #3. Read text file and split into PCollection of words.
        PCollection<String> words = pipeline.apply(TextIO.read().from(localFile))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));


        PCollection<String> sampleWords = words.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log words", ParDo.of(new LogStrings()));

        // Concept #4. Write PCollection to text file.
        sampleWords.apply(TextIO.write().withNumShards(1).to("C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\file1"));
        pipeline.run().waitUntilFinish();
    }

    public static class LogStrings extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing element: {}", c.element());
            c.output(c.element());
        }
    }

}
