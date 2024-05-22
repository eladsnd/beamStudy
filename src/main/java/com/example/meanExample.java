package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class meanExample {
    private static final Logger LOG = LoggerFactory.getLogger(com.example.meanExample.class);
    public static void main(String[] args) {
        // Create a Pipeline using the specified options.
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        // Create a PCollection With integer values ranging from 1 to 10
        PCollection<Integer> numbers_input = p.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // sum golbally
        PCollection<Double> mean = numbers_input.apply(Mean.globally());

        // log the output
        mean.apply(ParDo.of(new com.example.meanExample.LogFn()));

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
        PCollection<KV<String, Double>> sum_per_key = veg_input.apply(Mean.perKey());

        // log the output
        sum_per_key.apply(ParDo.of(new com.example.meanExample.LogFn2()));


        p.run().waitUntilFinish();

    }
    private static class LogFn2 extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Output: " + c.element());
        }
    }

    //log the output
    private static class LogFn extends DoFn<Double,Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Output: " + c.element());
        }
    }
}

