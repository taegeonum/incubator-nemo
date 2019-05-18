package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public final class WatermarkHoldStateCoder extends Coder<WatermarkHoldState> {

  public WatermarkHoldStateCoder() {
  }

  @Override
  public void encode(WatermarkHoldState value, OutputStream outStream) throws CoderException, IOException {
    final Instant instant = value.read();

    if (instant == null) {
      outStream.write(1);
    } else {
      outStream.write(0);
      SerializationUtils.serialize(instant, outStream);
    }

    final TimestampCombiner timestampCombiner = value.getTimestampCombiner();
    SerializationUtils.serialize(timestampCombiner, outStream);
  }

  @Override
  public WatermarkHoldState decode(InputStream inStream) throws CoderException, IOException {
    final int b = inStream.read();

    final Instant instant;
    if (b == 0) {
      instant = SerializationUtils.deserialize(inStream);
    } else {
      instant = null;
    }

    final TimestampCombiner timestampCombiner = SerializationUtils.deserialize(inStream);
    final WatermarkHoldState watermarkHoldState = new InMemoryStateInternals.InMemoryWatermarkHold<>(timestampCombiner);

    if (instant != null) {
      watermarkHoldState.add(instant);
    }

    return watermarkHoldState;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }
}