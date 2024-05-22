package com.example;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

public class sumExample {
    private static final Logger LOG = LoggerFactory.getLogger(sumExample.class);
    public static void main(String[] args) {
        // Create a Pipeline using the specified options.
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        // Create a PCollection With integer values ranging from 1 to 10
        PCollection<Integer> numbers_input = p.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // sum golbally
        PCollection<Integer> sum = numbers_input.apply(Sum.integersGlobally());

        // log the output
        sum.apply(ParDo.of(new LogFn()));

        // Create a PCollection With key value pairs
        PCollection<KV<String, Integer>> veg_input = p.apply(
                Create.of(KV.of("🥕", 3),
                        KV.of("🥕", 2),
                        KV.of("🍆", 1),
                        KV.of("🍅", 4),
                        KV.of("🍅", 5),
                        KV.of("🍆", 2),
                        KV.of("🍆", 3)));

        // sum per key
        PCollection<KV<String, Integer>> sum_per_key = veg_input.apply(Sum.integersPerKey());

        // log the output
        sum_per_key.apply(ParDo.of(new LogFn2()));


        p.run().waitUntilFinish();

    }
    static class LogFn2 extends DoFn<KV<String, Integer>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Output: " + c.element());
        }
    }

    //log the output
    static class LogFn extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Output: " + c.element());
        }
    }
}
