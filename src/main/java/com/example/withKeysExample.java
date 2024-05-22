package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class withKeysExample {
    public static void main(String[] args) {
        // Create a Pipeline using the specified options.
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);


        PCollection<String> input = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
        PCollection<KV<String, String>> lengthAndWord = input.apply(WithKeys.of(new SerializableFunction<String, String>() {
            @Override
            public String apply(String word) {
                return "ma man be tripping" + word.length();
            }
        }));

//        lengthAndWord.apply(ParDo.of(new LogFn()));


        PCollection<KV<String, String>> specifiedKeyAndWord = input.apply(WithKeys.of("SpecifiedKey"));

//        specifiedKeyAndWord.apply(ParDo.of(new LogFn()));

        PCollection<String> input2 = pipeline.apply(Create.of("apple", "banana", "cherry", "durian", "guava", "melon","ma man","ma boi"));

        PCollection<KV<String,String>> out = applyTransform(input2);

        out.apply(ParDo.of(new LogFn()));


        pipeline.run().waitUntilFinish();
    }

    private static class LogFn extends DoFn<KV<String, String>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("Output: " + c.element());
        }
    }

    // Thuis method groups the string collection with its first letter
    static PCollection<KV<String, String>> applyTransform(PCollection<String> input) {
        return input
                .apply(WithKeys.<String, String>of(fruit -> fruit.substring(0, 1))
                        .withKeyType(strings()));
    }
}
