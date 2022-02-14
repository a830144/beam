package com.chou.beam.util;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapToLines<K, T> extends PTransform<PCollection<KafkaRecord<K, T>>, PCollection<String>> {

	public static <K, V> MapToLines<K, V> of() {
		return new MapToLines<>(true);
	}

	public static <K, V> MapToLines<K, V> ofValuesOnly() {
		return new MapToLines<>(false);
	}

	private final boolean includeKey;

	MapToLines(boolean includeKey) {
		this.includeKey = includeKey;
	}

	@Override
	public PCollection<String> expand(PCollection<KafkaRecord<K, T>> input) {
		return input.apply(MapElements.into(TypeDescriptors.strings())
				.via(r -> includeKey ? ifNotNull(r.getKV().getKey()) + " " + ifNotNull(r.getKV().getValue())
						: ifNotNull(r.getKV().getValue())));
	}

	private static <T> String ifNotNull(T value) {
		return value != null ? value.toString() : "";
	}
}