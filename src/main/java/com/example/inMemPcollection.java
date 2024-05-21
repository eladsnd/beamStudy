package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class inMemPcollection {

    private static final Logger LOG = LoggerFactory.getLogger(inMemPcollection.class);

    public static void main(String[] args) {

        LOG.info("Running Task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words =
                pipeline.apply(
                        Create.of("To", "be", "or", "not", "to", "be","that", "is", "the", "question")
                );

        PCollection<Integer> numbers =
                pipeline.apply(
                        Create.of(1,2,3,4,5,6,7,8,9,10)
                );

        // add a prefix to each word
        words = words.apply(ParDo.of(new addPrefix()));

        words.apply("Log words", ParDo.of(new LogStrings()));

        numbers.apply("Log numbers", ParDo.of(new LogIntegers()));




        pipeline.run();
    }


    public static class LogStrings extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing word: {}", c.element());
            c.output(c.element());
        }
    }

    public static class addPrefix extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output("prefix-" + c.element());
        }
    }

    public static class LogIntegers extends DoFn<Integer, Integer> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing number: {}", c.element());
            c.output(c.element());
        }
    }
}