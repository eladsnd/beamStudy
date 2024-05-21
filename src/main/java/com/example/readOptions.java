package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class readOptions {

    private static final Logger LOG = LoggerFactory.getLogger(readOptions.class);

    public interface MyOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("C:\\Users\\Elad\\IdeaProjects\\beamStudy\\src\\main\\resources\\kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

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

        PCollection<String> output = pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(Filter.by(containsName("Reg")));

        // sum the number of lines that contain "William"
        output.apply(Count.globally())
                .apply(ParDo.of(new LogOutput<>()));

        output.apply("WriteLines", TextIO.write()
                .to(options.getOutput().replace(".txt", "")) // Remove extension to let Beam handle it
                .withSuffix(".txt")
                .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    // Custom filter function to keep lines that contain "William"
    static SerializableFunction<String, Boolean> containsName(String name) {
        return line -> line.contains(name);
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
