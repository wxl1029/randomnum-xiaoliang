package org.random.xl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Technical Assignment Requirements:
 * ------------------------------------
 * 1) Using Java 8, generate a million random numbers and output those numbers to a file.
 * 2) Create a program which sorts the output of your random number generator and sends the results to another file.
 */
public class Main {
  private static final String FILE_STEP_1 = "C:\\Users\\wxl10\\Desktop\\step1.txt";
  private static final String FILE_STEP_2 = "C:\\Users\\wxl10\\Desktop\\step2.txt";
  private static final int NUM_SIZE = 1000000;

  public static void main(String[] args) throws IOException {
    System.out.println("******************* Begin Main *******************");

    // task 1) - generate a million random numbers and output those numbers to a file
    generateRandomNumbers(NUM_SIZE);

    // task 2) - Create a program which sorts the output of your random number generator and sends the results to another file.
    MyFileReaderWriter rw = new MyFileReaderWriter(
        new File(FILE_STEP_1), new File(FILE_STEP_2), StandardCharsets.UTF_8, 10);
    rw.readSortWrite();
    
    System.out.println("******************* End Main *******************");
  }

  private static void generateRandomNumbers(int size) {
    // generate random numbers
    Instant start = Instant.now();
    StringBuilder sb = new StringBuilder();
    ThreadLocalRandom.current().ints(0, size)
        .limit(1000000)
        .forEach(s -> sb.append(s).append(System.getProperty("line.separator")));
    System.out.println("[Task#1] generate random numbers timeElapsed(ms) = " + Duration.between(start, Instant.now()).toMillis());

    // output to a file
    start = Instant.now();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_STEP_1))) {
      writer.write(sb.toString());
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
    System.out.println("[Task#1] output to a file timeElapsed(ms) = " + Duration.between(start, Instant.now()).toMillis());
  }

}