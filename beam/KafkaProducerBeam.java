package com.chou.beam;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptions.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerBeam {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PipelineOptions options = PipelineOptionsFactory.create();
		System.out.println("Runner: " + options.getRunner().getName());
		//options.setRunner();
        Pipeline p = Pipeline.create(options);

        // sample data
        List<KV<Long, String>> kvs = new ArrayList<>();
        kvs.add(KV.of(1L, "hi there"));
        kvs.add(KV.of(2L, "hi"));
        kvs.add(KV.of(3L, "hi sue bob"));
        kvs.add(KV.of(4L, "hi sue"));
        kvs.add(KV.of(5L, "hi bob"));

        PCollection<KV<Long, String>> input = p
                .apply(Create.of(kvs));

        input.apply(KafkaIO.<Long, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("helloworld")
                .withKeySerializer(LongSerializer.class)
                .withValueSerializer(StringSerializer.class)
        );

        p.run().waitUntilFinish();
	}

}
