package com.chou.beam.util;

import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Tokenize extends PTransform<PCollection<String>, PCollection<String>> {

  public static Tokenize of() {
    return new Tokenize();
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    PCollection<String> result =
        input.apply(FlatMapElements.into(TypeDescriptors.strings()).via(Utils::toWords));
    if (input.hasSchema()) {
      result.setSchema(
          input.getSchema(),
          input.getTypeDescriptor(),
          input.getToRowFunction(),
          input.getFromRowFunction());
    }
    return result;
  }
}