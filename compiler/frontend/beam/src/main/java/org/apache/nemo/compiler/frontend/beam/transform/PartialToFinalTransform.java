package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PartialToFinalTransform implements Transform<WindowedValue<KV>, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(PartialToFinalTransform.class.getName());

  private final Combine.CombineFn combineFn;
  private OutputCollector outputCollector;
  private Context context;

  public PartialToFinalTransform(Combine.CombineFn combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public void prepare(Context context, OutputCollector outputCollector) {
    this.context = context;
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(WindowedValue<KV> element) {
    final Object result = combineFn.extractOutput(element.getValue().getValue());

    LOG.info("Emitting output at {}: key {}, before extract: {} after extract; {}",
      context.getTaskId(), element.getValue().getKey(), element.getValue(),
      result);

    outputCollector.emit(element.withValue(KV.of(element.getValue().getKey(), result)));
  }

  @Override
  public void onWatermark(Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {

  }
}
