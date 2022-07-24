package org.random.xl;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * This class provides the functions that:
 * - sort the random numbers from an input file
 * - then send the results to another file
 */
public class MyFileReaderWriter {
  private final File randomFile; // input
  private final File sortedFile; // output
  private final long randomFileLengthMinus1;
  private final Charset charset;
  private final RandomAccessFile randomAccessFile; // read input file
  private final int bufferSize;
  private final List<BeginEndPair> beginEndPairs; // for input file
  private final int threadSize;
  private final ExecutorService threadPool;

  public MyFileReaderWriter(File randomFile, File sortedFile, Charset charset, int threadSize) throws FileNotFoundException {
    this.randomFile = randomFile;
    this.sortedFile = sortedFile;
    this.randomFileLengthMinus1 = randomFile.length() - 1;
    this.charset = charset;
    this.threadSize = threadSize;
    this.bufferSize = 1024 * 512;
    this.beginEndPairs = new ArrayList<>();
    this.randomAccessFile = new RandomAccessFile(randomFile, "r");
    this.threadPool = Executors.newFixedThreadPool(threadSize);
  }

  public void readSortWrite() {
    try {
      Instant start = Instant.now();

      // split the file content to multiple segments for multi-thread reading purpose
      long batchSize = this.randomFile.length() / this.threadSize;

      // each segment should not be truncated during the middle of the line,
      // it has to adjust begin/end position to avoid line breaking
      calcBeginEndPairs(0, batchSize);

      // use CompletableFuture to read and merge file segments, then sort
      List<String> sourceNumbers = multiRead();
      System.out.println("[Task#2.1] multiRead then sort timeElapsed(ms) = " + Duration.between(start, Instant.now()).toMillis());

      start = Instant.now();
      // for given sort numbers, divide evenly and log the segment content as well as
      // its start position when writing into new file
      List<ContentSegment> targetSegments = calcContentSegment(sourceNumbers);

      // use CompletableFuture to write all segments into target file
      multiWrite(targetSegments);
      System.out.println("[Task#2.2] multiWrite timeElapsed(ms) = " + Duration.between(start, Instant.now()).toMillis());
    } catch (IOException ex) {
      System.out.println("readSortWrite failed - " + ex.getMessage());
    } finally {
      if (this.threadPool != null && !this.threadPool.isShutdown()) {
        this.threadPool.shutdown();
      }
    }
  }

  private List<String> multiRead() {
    return this.beginEndPairs.stream()
        .map(pair -> CompletableFuture.supplyAsync(() -> {
          // partial lines that current thread can read
          List<String> partialLines = new ArrayList<>();

          try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // use MappedByteBuffer for better performance
            long totalLength = pair.getEnd() - pair.getBegin() + 1;
            MappedByteBuffer mapBuffer = this.randomAccessFile.getChannel().map(READ_ONLY, pair.getBegin(), totalLength);

            // totalLength = the all size current thread need to read
            // bufferSize = the real size current thread read each time
            for (int offset = 0; offset < totalLength; offset += this.bufferSize) {
              int readLength;
              if (offset + bufferSize <= totalLength) {
                readLength = bufferSize;
              } else {
                readLength = (int) (totalLength - offset);
              }

              // transfer bytes from mapBuffer into readBuffer
              byte[] readBuffer = new byte[this.bufferSize];
              mapBuffer.get(readBuffer, 0, readLength);

              // process each line and add it to the list
              for (int i = 0; i < readLength; i++) {
                byte b = readBuffer[i];
                if (b == '\n' || b == '\r') {
                  String line = new String(bos.toByteArray(), charset);
                  if (!"".equals(line)) {
                    partialLines.add(line);
                  }

                  bos.reset(); // reset for new line
                } else {
                  bos.write(b);
                }
              }
            }
            return partialLines;
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }, this.threadPool).whenComplete((v, e) -> {
          if (e == null) {
            System.out.println(Thread.currentThread().getName() + " - success");
          } else {
            System.out.println(Thread.currentThread().getName() + " - fail - " + e.getMessage());
          }
        }).exceptionally(e -> {
          System.out.println(e.getMessage());
          return new ArrayList<>();
        }))
        .collect(Collectors.toList())
        .stream()
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .mapToInt(Integer::parseInt)
        .sorted()
        .mapToObj(String::valueOf)
        .collect(Collectors.toList());
  }

  private void multiWrite(List<ContentSegment> targetSegments) {
    targetSegments.stream().map(segment -> CompletableFuture.runAsync(() -> {
          FileLock lock;

          try (FileOutputStream fos = new FileOutputStream(this.sortedFile)) {
            FileChannel fileChannel = fos.getChannel();
            while (true) {
              try {
                lock = fileChannel.tryLock();
                break;
              } catch (OverlappingFileLockException | IOException e) {
                // file is currently locked by other thread, just keep spinning
              }
            }
            if (lock != null) {
              ByteBuffer buffer = ByteBuffer.wrap(segment.getContent().getBytes());
              fileChannel.write(buffer, segment.getStartPos());
              if (lock.isValid()) {
                lock.release();
              }
            }
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }, this.threadPool).whenComplete((v, e) -> {
          if (e == null) {
            System.out.println(Thread.currentThread().getName() + " - success");
          } else {
            System.out.println(Thread.currentThread().getName() + " - fail - " + e.getMessage());
          }
        }).exceptionally(e -> {
          System.out.println(e.getMessage());
          return null;
        })).collect(Collectors.toList())
        .stream()
        .map(CompletableFuture::join);
  }

  private void calcBeginEndPairs(long begin, long batchSize) throws IOException {
    if (begin > this.randomFileLengthMinus1) {
      return;
    }

    long adjustedEnd = begin + batchSize - 1;
    // if reach the end of the file, add the last segment
    if (adjustedEnd >= this.randomFileLengthMinus1) {
      this.beginEndPairs.add(new BeginEndPair(begin, randomFileLengthMinus1));
      return;
    }

    // each segment should not be truncated during the middle of the line,
    // so, the end position need to be adjusted according to the line break
    this.randomAccessFile.seek(adjustedEnd);
    byte b = (byte) this.randomAccessFile.read();
    while (b != '\n' && b != '\r') {
      adjustedEnd++;
      if (adjustedEnd >= this.randomFileLengthMinus1) {
        break;
      }
      this.randomAccessFile.seek(adjustedEnd);
      b = (byte) this.randomAccessFile.read();
    }

    // add the new calculated [begin, end]
    this.beginEndPairs.add(new BeginEndPair(begin, adjustedEnd));

    // continue with the rest of the file content
    calcBeginEndPairs(adjustedEnd + 1, batchSize);
  }

  private List<ContentSegment> calcContentSegment(List<String> sourceNumbers) {
    List<ContentSegment> segments = new ArrayList<>();

    int max = sourceNumbers.size();
    int batch = max / 10;
    int start = 0;
    int pos = 0;
    while (start < max) {
      int fromIdx = start; // inclusive
      int toIdx = (start + batch <= max) ? start + batch : max; // exclusive

      ContentSegment cs = new ContentSegment(sourceNumbers.subList(fromIdx, toIdx), pos);
      segments.add(cs);
      start += batch;
      pos += cs.getLength();
    }

    return segments;
  }

}
