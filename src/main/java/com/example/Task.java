package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public interface MyOptions extends PipelineOptions {
        // Default value if [--inputFile] is null
        @Description("Path of the file to read from")
        @Default.String("C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

        // Set this required option to specify where to write the output
        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\file.txt")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        readLines(options);
    }

    static void readLines(MyOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // Read lines from the input file
        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(Filter.by((String line) -> !line.isEmpty()));

        // Log each line
        lines.apply("Log", ParDo.of(new LogOutput<>()));

        // Write lines to the output file
        lines.apply("WriteLines", TextIO.write().to(options.getOutput())
                .withNumShards(1)
                .withSuffix(".txt"));

        // Execute the pipeline
        pipeline.run().waitUntilFinish();
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
