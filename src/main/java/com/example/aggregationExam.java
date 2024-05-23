package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class aggregationExam {
    /*You are provided with a PCollection created from the array of taxi order prices in a csv file.
    Your task is to find how many orders are below $15 and how many are equal to or above $15.
    Return it as a map structure (key-value), make above or below the key, and the total dollar value (sum) of orders
    - the value. Although there are many ways to do this, try using another transformation presented in this module.
    Here is a small list of fields and an example record from this dataset*/
    String csvFile = "C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\sample1000.csv";
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    //Parse the csv file and create a PCollection
    PCollection<String> input = pipeline.apply("ReadCsvFile",TextIO.read().from(csvFile));
    PCollection<Double> rideTolalAmounts = input.apply("ExtractTaxiRideCostFn", ParDo.of(new ExtractTaxiRideCostFn()));

    //add key-value pairs to the PCollection to indicate whether the order is above or below $15
    PCollection<KV<AddKeyFn, Double>> keyedPrices  = rideTolalAmounts.apply("AddKey", WithKeys.of(new AddKeyFn()));

    //Count the number of orders above and below $15
    PCollection<KV<AddKeyFn, Double>> aggregatedValues  = keyedPrices.apply("AggregateValues", Combine.perKey(Sum.ofDoubles()));

    //






    private static class ExtractTaxiRideCostFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }

        private static Double tryParseTaxiRideCost(String[] inputItems) {try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }}

        private static String tryParseString(String[] inputItems, int index) {
            return inputItems.length > index ? inputItems[index] : null;
        }
    }


    private static class AddKeyFn extends DoFn<Double, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Double totalAmount = c.element();
            String key = totalAmount < 15 ? "below" : "above";
            c.output(totalAmount);
        }
    }
}

