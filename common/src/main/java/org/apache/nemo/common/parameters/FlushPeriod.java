package org.apache.nemo.common.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "flush period (ms)", default_value = "200")
public final class FlushPeriod implements Name<Integer> {
}
