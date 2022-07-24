package org.random.xl;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Specify the content segment when writing into target file
 */
public class ContentSegment {
  private final String content;
  private final long length;
  private final long startPos;

  public String getContent() {
    return content;
  }

  public long getLength() {
    return length;
  }

  public long getStartPos() {
    return startPos;
  }

  public ContentSegment(List<String> subList, long startPos) {
    this.content = subList.stream()
        .collect(Collectors.joining(System.getProperty("line.separator")))
        .concat(System.getProperty("line.separator"));
    this.length = content.length();
    this.startPos = startPos;
  }
}
