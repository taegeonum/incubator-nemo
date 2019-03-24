package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class HandleDataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(HandleDataFetcher.class.getName());

  private final ScheduledExecutorService pollingTrigger = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService executorService;
  private final List<DataFetcher> fetchers;
  private boolean pollingTime;
  private final int pollingInterval = 200; // ms

  private boolean closed = false;
  private final OffloadingResultCollector resultCollector;

  private int processedCnt = 0;


  public HandleDataFetcher(final List<DataFetcher> fetchers,
                           final OffloadingResultCollector resultCollector) {
    LOG.info("Handle data fetcher start");
    this.executorService = Executors.newSingleThreadExecutor();
    this.resultCollector = resultCollector;
    this.fetchers = fetchers;
    this.pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, 200, 200, TimeUnit.MILLISECONDS);
  }

  public void start() {

    if (fetchers.size() <= 0) {
      throw new RuntimeException("Fetcher size is zero..");
    }

    executorService.execute(() -> {
      final List<DataFetcher> availableFetchers = new LinkedList<>(fetchers);
      final List<DataFetcher> pendingFetchers = new LinkedList<>();

      // empty means we've consumed all task-external input data
      while (!closed) {

        // We first fetch data from available data fetchers
        final Iterator<DataFetcher> availableIterator = availableFetchers.iterator();

        while (availableIterator.hasNext()) {

          final DataFetcher dataFetcher = availableIterator.next();
          try {
            //final long a = System.currentTimeMillis();
            final Object element = dataFetcher.fetchDataElement();
            onEventFromDataFetcher(element, dataFetcher);

            if (element instanceof Finishmark) {
              availableIterator.remove();
            }
          } catch (final NoSuchElementException e) {
            // No element in current data fetcher, fetch data from next fetcher
            // move current data fetcher to pending.
            availableIterator.remove();
            pendingFetchers.add(dataFetcher);
          } catch (final IOException e) {
            e.printStackTrace();
            LOG.error("{} Execution Failed (Recoverable: input read failure)!");
            throw new RuntimeException(e);
          }
        }

        final Iterator<DataFetcher> pendingIterator = pendingFetchers.iterator();

        if (pollingTime) {
          // We check pending data every polling interval
          pollingTime = false;

          while (pendingIterator.hasNext()) {
            final DataFetcher dataFetcher = pendingIterator.next();
            try {
              //final long a = System.currentTimeMillis();
              final Object element = dataFetcher.fetchDataElement();
              //fetchTime += (System.currentTimeMillis() - a);

              //final long b = System.currentTimeMillis();
              onEventFromDataFetcher(element, dataFetcher);
              // processingTime += (System.currentTimeMillis() - b);

              // We processed data. This means the data fetcher is now available.
              // Add current data fetcher to available
              pendingIterator.remove();
              if (!(element instanceof Finishmark)) {
                availableFetchers.add(dataFetcher);
              }

            } catch (final NoSuchElementException e) {
              // The current data fetcher is still pending.. try next data fetcher
            } catch (final IOException e) {
              // IOException means that this task should be retried.
              e.printStackTrace();
              LOG.error("{} Execution Failed (Recoverable: input read failure)!");
              throw new RuntimeException(e);
            }
          }

          if (processedCnt > 0) {
            // flush data
            resultCollector.flush(-1);
          }
          processedCnt = 0;
        }

        // If there are no available fetchers,
        // Sleep and retry fetching element from pending fetchers every polling interval
        if (availableFetchers.isEmpty() && !pendingFetchers.isEmpty()) {
          try {
            Thread.sleep(pollingInterval);
          } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      }

      LOG.info("Closed handle datafetcher");

      if (closed) {
        if (processedCnt > 0) {
          // flush data
          resultCollector.flush(-1);
        }
      }


      // send checkpoint mark to the VM!!
      final SourceVertexDataFetcher dataFetcher = (SourceVertexDataFetcher) fetchers.get(0);
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) dataFetcher.getReadable();
      final UnboundedSource.CheckpointMark checkpointMark = readable.getReader().getCheckpointMark();
      LOG.info("Send checkpointmark to vm: {}", checkpointMark);
      resultCollector.collector.emit(checkpointMark);

      // Close all data fetchers
      fetchers.forEach(fetcher -> {
        try {
          fetcher.close();
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });

    });
  }

  public void close() throws Exception {
    closed = true;
    executorService.shutdown();
    LOG.info("Await termination..");
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    LOG.info("End of await termination..");
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   * @param event event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof Watermark) {
      // Watermark
      //LOG.info("Watermark: {}", event);
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // Process data element
      //LOG.info("Data: {}", event);
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }


  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector,
                              final TimestampAndValue dataElement) {
    processedCnt += 1;
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                       final Watermark watermark) {
    processedCnt += 1;
    outputCollector.emitWatermark(watermark);
  }
}
