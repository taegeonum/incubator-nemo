package org.apache.nemo.common.test;

import java.io.Serializable;

public final class TCPSendEvent implements Serializable {

  public final EventOrWatermark event;
  public TCPSendEvent(final EventOrWatermark event) {
    this.event = event;
  }
}