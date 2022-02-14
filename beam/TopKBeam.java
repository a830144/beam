package com.chou.beam;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdks.java.io.kafka.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import com.chou.beam.util.MapToLines;
import com.chou.beam.util.Tokenize;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import lombok.Value;

public class TopKBeam {
	public static void main(String[] args) {
	    Params params = parseArgs(args);
	    PipelineOptions options = PipelineOptionsFactory.create();
	    //PipelineOptions options = PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create();
	    Pipeline pipeline = Pipeline.create(options);
	    PCollection<String> lines =
	        pipeline
	            .apply(
	                KafkaIO.<String, String>read()
	                    .withBootstrapServers(params.getBootstrapServer())
	                    .withKeyDeserializer(StringDeserializer.class)
	                    .withValueDeserializer(StringDeserializer.class)
	                    .withTopic(params.getInputTopic())
	                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest")))
	            .apply(MapToLines.of());
	    PCollection<KV<String, Long>> output =
	        countWordsInFixedWindows(lines, params.getWindowLength(), params.getK());
	    output.apply(
	        KafkaIO.<String, Long>write()
	            .withBootstrapServers(params.getBootstrapServer())
	            .withTopic(params.getOutputTopic())
	            .withKeySerializer(StringSerializer.class)
	            .withValueSerializer(LongSerializer.class));
	    pipeline.run().waitUntilFinish();
	  }

	  @VisibleForTesting
	  static PCollection<KV<String, Long>> countWordsInFixedWindows(
	      PCollection<String> lines, Duration windowLength, int k) {

	    return lines
	        .apply(Window.into(FixedWindows.of(windowLength)))
	        .apply(Tokenize.of())
	        .apply(Count.perElement())
	        .apply(
	            Top.of(
	                    k,
	                    (Comparator<KV<String, Long>> & Serializable)
	                        (a, b) -> Long.compare(a.getValue(), b.getValue()))
	                .withoutDefaults())
	        .apply(Flatten.iterables());
	  }

	  @VisibleForTesting
	  static Params parseArgs(String[] args) {
	    if (args.length < 5) {
	      throw new IllegalArgumentException(
	          "Expected at least 5 arguments: <windowLength> <bootstrapServer> <inputTopic> <outputTopic> <k>");
	    }
	    return new Params(
	        Duration.standardSeconds(Integer.parseInt("5")),
	        "[::1]:9092",
	        "quickstart-events",
	        "quickstart-events",
	        Integer.parseInt("5"),
	        Arrays.copyOfRange(args, 5, args.length));
	  }

	  @Value
	  @VisibleForTesting
	  static class Params {
	    
		public Params(Duration windowLength, String bootstrapServer, String inputTopic, String outputTopic, int k,
				String[] remainingArgs) {
			super();
			this.windowLength = windowLength;
			this.bootstrapServer = bootstrapServer;
			this.inputTopic = inputTopic;
			this.outputTopic = outputTopic;
			this.k = k;
			this.remainingArgs = remainingArgs;
		}
		public Duration getWindowLength() {
			return windowLength;
		}
		public void setWindowLength(Duration windowLength) {
			this.windowLength = windowLength;
		}
		public String getBootstrapServer() {
			return bootstrapServer;
		}
		public void setBootstrapServer(String bootstrapServer) {
			this.bootstrapServer = bootstrapServer;
		}
		public String getInputTopic() {
			return inputTopic;
		}
		public void setInputTopic(String inputTopic) {
			this.inputTopic = inputTopic;
		}
		public String getOutputTopic() {
			return outputTopic;
		}
		public void setOutputTopic(String outputTopic) {
			this.outputTopic = outputTopic;
		}
		public int getK() {
			return k;
		}
		public void setK(int k) {
			this.k = k;
		}
		public String[] getRemainingArgs() {
			return remainingArgs;
		}
		public void setRemainingArgs(String[] remainingArgs) {
			this.remainingArgs = remainingArgs;
		}
		Duration windowLength;
	    String bootstrapServer;
	    String inputTopic;
	    String outputTopic;
	    int k;
	    String[] remainingArgs;
	  }

}
