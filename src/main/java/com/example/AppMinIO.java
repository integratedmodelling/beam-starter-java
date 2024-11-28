// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.io.aws2.s3.DefaultS3ClientBuilderFactory;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 * <p>
 * This class, {@link WordCount}, is the second in a series of four successively
 * more detailed 'word count' examples. You may first want to take a look at
 * {@link MinimalWordCount}. After you've looked at this example, then see the
 * {@link DebuggingWordCount} pipeline, for introduction of additional concepts.
 *
 * <p>
 * For a detailed walkthrough of this example, see
 * <a href="https://beam.apache.org/get-started/wordcount-example/">
 * https://beam.apache.org/get-started/wordcount-example/ </a>
 *
 * <p>
 * Basic concepts, also in the MinimalWordCount example: Reading text files;
 * counting a PCollection; writing to text files
 *
 * <p>
 * New Concepts:
 *
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>
 * Concept #1: you can execute this pipeline either locally or using by
 * selecting another runner. These are now command-line options and not
 * hard-coded as they were in the MinimalWordCount example.
 *
 * <p>
 * To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * <p>
 * To execute this pipeline, specify a local output file (if using the
 * {@code DirectRunner}) or output prefix on a supported distributed file
 * system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>
 * The input file defaults to a public data set containing the text of King
 * Lear, by William Shakespeare. You can override it and choose your own input
 * with {@code --inputFile}.
 */
public class AppMinIO {
	public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

	/**
	 * Concept #2: You can make your pipeline assembly code less verbose by defining
	 * your DoFns statically out-of-line. This DoFn tokenizes lines of text into
	 * individual words; we pass it to a ParDo in the pipeline.
	 */
	// [START extract_words_fn]
	static class ExtractWordsFn extends DoFn<String, String> {
		private static final long serialVersionUID = -5513785986746146713L;
		private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
		private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<String> receiver) {
			lineLenDist.update(element.length());
			if (element.trim().isEmpty()) {
				emptyLines.inc();
			}

			// Split the line into words.
			String[] words = element.split(AppMinIO.TOKENIZER_PATTERN, -1);

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					receiver.output(word);
				}
			}
		}
	}
	// [END extract_words_fn]

	/** A SimpleFunction that converts a Word and Count into a printable string. */
	public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
		private static final long serialVersionUID = 236177811985039796L;

		@Override
		public String apply(KV<String, Long> input) {
			return input.getKey() + ": " + input.getValue();
		}
	}


/**
 * A PTransform that converts a PCollection containing lines of text into a
 * PCollection of formatted word counts.
 *
 * <p>
 * Concept #3: This is a custom composite transform that bundles two transforms
 * (ParDo and Count) as a reusable PTransform subclass. Using composite
 * transforms allows for easy reuse, modular testing, and an improved monitoring
 * experience.
 */
// [START count_words]
public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
	private static final long serialVersionUID = -3579681967183499444L;

	@Override
	public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

		// Convert lines of text into individual words.
		PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

		// Count the number of times each word occurs.
		PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

		return wordCounts;
	}
}
// [END count_words]

/**
   * Options supported by {@link WordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
   * be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  // [START wordcount_options]
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("s3://public-s3-test/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);
  }
  // [END wordcount_options]

  static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(new CountWords())
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }
  
  private static class PathStyleAccessClientBuilderFactory extends DefaultS3ClientBuilderFactory {
	  @Override
	  public S3ClientBuilder createBuilder(S3Options s3Options) {
		  S3ClientBuilder s3ClientBuilder =  super.createBuilder(s3Options);
		  s3ClientBuilder.forcePathStyle(true); // Required for MinIO
		  return s3ClientBuilder;
	  }
  }
    
  public static void main(String[] args) throws Exception {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).as(WordCountOptions.class);
    
    options.as(S3Options.class).setS3ClientFactoryClass(PathStyleAccessClientBuilderFactory.class);
    
    runWordCount(options);
  }
}
