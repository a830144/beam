package com.chou.beam;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdks.java.io.kafka.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.chou.beam.util.MapToLines;
import com.chou.beam.util.Tokenize;
import com.google.common.annotations.VisibleForTesting;

import lombok.Value;

public class AverageWordLengthBeam {
	public static void main(String[] args) {
		Params params = parseArgs(args);
		PipelineOptions options = PipelineOptionsFactory.create();
		//Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());
		Pipeline pipeline = Pipeline.create(options);
		PCollection<String> words = pipeline.apply(KafkaIO.<String, String>read()
				.withBootstrapServers(params.getBootstrapServer()).withKeyDeserializer(StringDeserializer.class)
				.withValueDeserializer(StringDeserializer.class).withTopic(params.getInputTopic())
				.updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest")))
				.apply(MapToLines.of()).apply(Tokenize.of());

		calculateAverageWordLength(words, params.isDisableDefaults()).apply(WithKeys.of(""))
				.apply(KafkaIO.<String, Double>write().withBootstrapServers(params.getBootstrapServer())
						.withTopic(params.getOutputTopic()).withKeySerializer(StringSerializer.class)
						.withValueSerializer(DoubleSerializer.class));

		pipeline.run().waitUntilFinish();
	}

	@VisibleForTesting
	static PCollection<Double> calculateAverageWordLength(PCollection<String> words) {
		return calculateAverageWordLength(words, false);
	}

	static PCollection<Double> calculateAverageWordLength(PCollection<String> words, boolean disableDefaults) {
		return words
				.apply(Window.<String>into(new GlobalWindows())
						.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))).accumulatingFiredPanes()
						.withOnTimeBehavior(OnTimeBehavior.FIRE_IF_NON_EMPTY))
				.apply(disableDefaults ? Combine.globally(new AverageFn()).withoutDefaults()
						: Combine.globally(new AverageFn()));
	}

	static Params parseArgs(String[] args) {
		/*if (args.length < 3) {
			throw new IllegalArgumentException(
					"Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
		}*/
		int pipelinesArgsIndex = 3;
		boolean disableDefaults = args.length > pipelinesArgsIndex
				&& args[pipelinesArgsIndex].equals("--withoutDefaults");
		if (disableDefaults) {
			pipelinesArgsIndex++;
		}
		return new Params(
				"[::1]:9092",
		        "quickstart-events",
		        "averageWordLength", 
				Arrays.copyOfRange(args, pipelinesArgsIndex, args.length));
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
		public boolean isDisableDefaults() {
			return disableDefaults;
		}
		public void setDisableDefaults(boolean disableDefaults) {
			this.disableDefaults = disableDefaults;
		}
		public String[] getRemainingArgs() {
			return remainingArgs;
		}
		public void setRemainingArgs(String[] remainingArgs) {
			this.remainingArgs = remainingArgs;
		}
		public Params(String bootstrapServer, String inputTopic, String outputTopic, boolean disableDefaults,
				String[] remainingArgs) {
			super();
			this.bootstrapServer = bootstrapServer;
			this.inputTopic = inputTopic;
			this.outputTopic = outputTopic;
			this.disableDefaults = disableDefaults;
			this.remainingArgs = remainingArgs;
		}
		String bootstrapServer;
		String inputTopic;
		String outputTopic;
		boolean disableDefaults;
		String[] remainingArgs;
	}

	static class AverageFn extends CombineFn<String, AverageAccumulator, Double> {
		@Override
		public AverageAccumulator createAccumulator() {
			return new AverageAccumulator(0, 0);
		}

		@Override
		public AverageAccumulator addInput(AverageAccumulator accumulator, String input) {
			return new AverageAccumulator(accumulator.getSumLength() + input.length(), accumulator.getCount() + 1);
		}

		@Override
		public AverageAccumulator mergeAccumulators(Iterable<AverageAccumulator> accumulators) {
			long sumLength = 0L;
			long count = 0L;
			for (AverageAccumulator acc : accumulators) {
				sumLength += acc.getSumLength();
				count += acc.getCount();
			}
			return new AverageAccumulator(sumLength, count);
		}

		@Override
		public Double extractOutput(AverageAccumulator accumulator) {
			return accumulator.getSumLength() / (double) accumulator.getCount();
		}

		@Override
		public Coder<AverageAccumulator> getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder) {

			return new AverageAccumulatorCoder();
		}
	}

	@Value
	static class AverageAccumulator {
		public AverageAccumulator(long sumLength, long count) {
			super();
			this.sumLength = sumLength;
			this.count = count;
		}

		public long getSumLength() {
			return sumLength;
		}

		public void setSumLength(long sumLength) {
			this.sumLength = sumLength;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		long sumLength;
		long count;
	}

	static class AverageAccumulatorCoder extends CustomCoder<AverageAccumulator> {

		@Override
		public void encode(AverageAccumulator value, OutputStream outStream) throws CoderException, IOException {

			VarInt.encode(value.getSumLength(), outStream);
			VarInt.encode(value.getCount(), outStream);
		}

		@Override
		public AverageAccumulator decode(InputStream inStream) throws CoderException, IOException {
			return new AverageAccumulator(VarInt.decodeLong(inStream), VarInt.decodeLong(inStream));
		}
	}
}
