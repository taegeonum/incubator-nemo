package org.apache.nemo.runtime.master;

import java.util.*;

public class YarnVMAddresses {

  public static List<String> findDuplicateAddress(final Set<String> s, final List<String> l) {
    final List<String> dupL = new ArrayList<>(l);

    for (final String ss : s) {
      dupL.remove(ss);
    }

    return dupL;
  }

  public static void check() {
    final Set<String> addressSet = new HashSet<>(VM_ADDRESSES);

    if (addressSet.size() != VM_ADDRESSES.size()) {
      throw new RuntimeException("duplicate address! " + findDuplicateAddress(addressSet, VM_ADDRESSES));
    }

    final Set<String> idSet = new HashSet<>(INSTANCE_IDS);

    if (idSet.size() != INSTANCE_IDS.size()) {
      throw new RuntimeException("duplicate id! " + findDuplicateAddress(idSet, INSTANCE_IDS));
    }
  }

  public static final List<String> VM_ADDRESSES =
    Arrays.asList(
      "172.31.28.29", // 11
      "172.31.26.92",
      "172.31.19.171",
      "172.31.23.120",
      "172.31.18.248" // 15
    );

  public static final List<String> INSTANCE_IDS =
    Arrays.asList(
      "i-01704c3fff8200787", // 11
      "i-084872ac35829b095",
      "i-09c443ab075702811",
      "i-0c6f62a5fb92d706d",
      "i-0ea2eb7a4928408db" // 15
    );
}
