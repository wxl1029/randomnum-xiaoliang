package org.random.xl;

/**
 * Specify the begin/end position when reading file content
 */
public class BeginEndPair {
  private final long begin;
  private final long end;

  public long getBegin() {
    return begin;
  }

  public long getEnd() {
    return end;
  }

  public BeginEndPair(long begin, long end) {
    this.begin = begin;
    this.end = end;
  }
}
