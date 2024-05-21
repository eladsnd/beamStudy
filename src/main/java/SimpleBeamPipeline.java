
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SimpleBeamPipeline {

    public static void main(String[] args) {
        // Step 1: Define pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();

        // Step 2: Create the pipeline
        Pipeline p = Pipeline.create(options);

        // Step 3: Apply transforms
        PCollection<String> output = createPCollection(p);

        output.apply("Log", ParDo.of(new LogOutput<String>()));

        p.run().waitUntilFinish();
    }

    private static PCollection<String> createPCollection(Pipeline p) {
        return p.apply(Create.of("Hello World"));
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element: ";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(prefix + c.element());
            c.output(c.element());
        }
    }
}
