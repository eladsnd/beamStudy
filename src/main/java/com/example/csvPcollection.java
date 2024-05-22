package com.example;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;

public class csvPcollection {
    private static final Logger LOG = LoggerFactory.getLogger(csvPcollection.class);


    public static void main(String[] args) {
        String localFile = "C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\sample1000.csv";
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> input = pipeline.apply(TextIO.read().from(localFile));

        PCollection<Double> rideTolalAmounts = input.apply(ParDo.of(new ExtractTaxiRideCostFn()));
        final PTransform<PCollection<Double>, PCollection<Iterable<Double>>> sample = Sample.fixedSizeGlobally(10);
        PCollection<Double> rideAmounts = rideTolalAmounts.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log amounts", ParDo.of(new LogDouble()));

        pipeline.run().waitUntilFinish();

    }

    private static Double tryParseTaxiRideCost(String[] inputItems) {
        try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }
    }


    private static String tryParseString(String[] inputItems, int index) {
        return inputItems.length > index ? inputItems[index] : null;
    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }
    }

    public static class LogDouble extends DoFn<Double, Double> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Total Amount: {}", c.element());
            c.output(c.element());
        }
    }
}


