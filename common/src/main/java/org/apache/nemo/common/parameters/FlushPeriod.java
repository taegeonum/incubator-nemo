package org.apache.nemo.common.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "flush period (ms)")
public final class FlushPeriod implements Name<Integer> {
}
