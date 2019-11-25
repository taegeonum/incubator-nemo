package org.apache.nemo.runtime.master;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

public final class VMManager {

  private static final Logger LOG = LoggerFactory.getLogger(VMManager.class.getName());
  private final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

  private final List<String> runningVMs = new ArrayList<>();

  @Inject
  private VMManager() {
  }

  public synchronized void startInstances(final int num) {

    final List<String> readyVMs = new ArrayList<>(VMScalingAddresses.INSTANCE_IDS);
    readyVMs.removeAll(runningVMs);

    // select
    final Iterator<String> iterator = readyVMs.iterator();
    int instanceNum = 0;


    final List<String> startIds = new ArrayList<>(num);

    while (iterator.hasNext() && instanceNum < num) {
      final String readyVmId = iterator.next();
      runningVMs.add(readyVmId);
      startIds.add(readyVmId);
      instanceNum += 1;
    }

    final StartInstancesRequest startRequest = new StartInstancesRequest()
      .withInstanceIds(startIds);
    ec2.startInstances(startRequest);
    LOG.info("Starting ec2 instances {}/{}", startIds, System.currentTimeMillis());
  }

  private void stopInstances(final Collection<String> instanceIds) {
    LOG.info("Stopping instances {}", instanceIds);
    final StopInstancesRequest request = new StopInstancesRequest()
      .withInstanceIds(instanceIds);
    ec2.stopInstances(request);
  }

  private void stopInstancesWithAddresses(final Collection<String> addresses) {
    LOG.info("Stopping instances {}", addresses);

    final Collection<String> instanceIds = addresses.stream()
      .map(address ->VMScalingAddresses.INSTANCE_IDS
        .get(VMScalingAddresses.VM_ADDRESSES.indexOf(address)))
      .collect(Collectors.toList());

    stopInstances(instanceIds);
  }
}
