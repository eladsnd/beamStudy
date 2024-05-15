import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> lines = pipeline.apply(TextIO.read().from("resources/file"));

        PCollection<String> words = lines.apply(FlatMapElements
                .into(TypeDescriptor.of(String.class))
                .via((String line) -> Arrays.asList(line.split("\\W+"))));

        PCollection<Long> wordCounts = words.apply(Count.globally());

        wordCounts.apply(MapElements
                        .into(TypeDescriptor.of(String.class))
                        .via((Long count) -> "Total words: " + count))
                .apply(TextIO.write().to("resources/file1.txt").withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
