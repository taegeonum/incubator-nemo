package org.apache.nemo.runtime.executor;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nemo.runtime.common.HDFSUtils;
import org.apache.nemo.common.StateStore;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HDFStateStore implements StateStore {
  private static final Logger LOG = LoggerFactory.getLogger(HDFStateStore.class.getName());

  private final Configuration conf = HDFSUtils.CONF;

  @Inject
  public HDFStateStore() throws IOException {
  }

  @Override
  public InputStream getStateStream(String taskId) {
    final Path path = new Path(HDFSUtils.STATE_PATH + "/" +  taskId);
    try {
      final FileSystem fileSystem = path.getFileSystem(conf);
      return fileSystem.open(path);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream getOutputStreamForStoreTaskState(String taskId) {
    LOG.info("Storing task state " + taskId + " into HDFS");
    final Path path = new Path(HDFSUtils.STATE_PATH + "/" +  taskId);
    try {
      final FileSystem fileSystem = path.getFileSystem(conf);

      if (fileSystem.exists(path)) {
        LOG.info("Task state " + taskId + " already exist.. remove and rewrite");
        fileSystem.delete(path, true);
      }

      final FSDataOutputStream out = fileSystem.create(path);
      return out;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean containsState(String taskId) {
    final Path path = new Path(HDFSUtils.STATE_PATH + "/" +  taskId);
    try {
      final FileSystem fileSystem = path.getFileSystem(conf);
      return fileSystem.exists(path);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
