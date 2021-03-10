package org.apache.nemo.offloading.common;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * This class is used for reading objects from external JARs.
 */
public final class CustomClassLoader extends ClassLoader {
  private static final Logger LOG = Logger.getLogger(CustomClassLoader.class);

  private ClassLoader classLoader;

  public CustomClassLoader(final ClassLoader classLoader) throws IOException {
    super();
    this.classLoader = classLoader;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    System.out.println("Class Loading Started for " + name);
    if (name.contains("org/apache/beam/repackaged")) {
      final String[] splited = name.split("/");
      final StringBuilder nameBuilder = new StringBuilder();
      for (int i = 5; i < splited.length; i++) {
        nameBuilder.append(splited[i]);

        if (i != splited.length - 1) {
          nameBuilder.append("/");
        }
      }

      final String changeName = nameBuilder.toString();
      LOG.info("Change class name from " + name + " to " + changeName);
      return classLoader.loadClass(changeName);
    } else {
      return classLoader.loadClass(name);
    }
  }
}
