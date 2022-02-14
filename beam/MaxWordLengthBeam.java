package com.chou.beam;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdks.java.io.kafka.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import com.chou.beam.util.MapToLines;
import com.chou.beam.util.Tokenize;

import lombok.Value;
public class MaxWordLengthBeam {
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
	    PCollection<String> output = computeLongestWord(lines);
	    output
	        .apply(
	            MapElements.into(
	                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
	                .via(e -> KV.of("", e)))
	        .apply(
	            KafkaIO.<String, String>write()
	                .withBootstrapServers(params.getBootstrapServer())
	                .withTopic(params.getOutputTopic())
	                .withKeySerializer(StringSerializer.class)
	                .withValueSerializer(StringSerializer.class));
	    pipeline.run().waitUntilFinish();
	  }

	  static PCollection<String> computeLongestWord(PCollection<String> lines) {
	    return lines
	        .apply(Tokenize.of())
	        .apply(
	            Window.<String>into(new GlobalWindows())
	                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
	                .withAllowedLateness(Duration.ZERO)
	                .accumulatingFiredPanes())
	        .apply(
	            Max.globally(
	                (Comparator<String> & Serializable)
	                    (a, b) -> Long.compare(a.length(), b.length())));
	  }

	  static Params parseArgs(String[] args) {
	    /*if (args.length < 3) {
	      throw new IllegalArgumentException(
	          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
	    }*/
	    return new Params(
	    		"[::1]:9092",
		        "quickstart-events",
		        "maxWordLength", 
	    		Arrays.copyOfRange(args, 3, args.length));
	  }

	  @Value
	  static class Params {
	    public Params(String bootstrapServer, String inputTopic, String outputTopic, String[] remainingArgs) {
			super();
			this.bootstrapServer = bootstrapServer;
			this.inputTopic = inputTopic;
			this.outputTopic = outputTopic;
			this.remainingArgs = remainingArgs;
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
		public String[] getRemainingArgs() {
			return remainingArgs;
		}
		public void setRemainingArgs(String[] remainingArgs) {
			this.remainingArgs = remainingArgs;
		}
		String bootstrapServer;
	    String inputTopic;
	    String outputTopic;
	    String[] remainingArgs;
	  }
}
