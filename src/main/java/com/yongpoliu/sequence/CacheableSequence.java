package com.yongpoliu.sequence;

import java.util.Map;

import lombok.Data;

@Data
public class CacheableSequence implements Sequence {

  private final String name;

  private long currentValue;

  private long maxValue;

  private SequenceFactory sequenceFactory;

  public synchronized long nextSeq() {
    if (isNoSeqAvailable()) {
      refreshSequence();
    }

    return currentValue ++;
  }

  private boolean isNoSeqAvailable() {
    return currentValue >= maxValue;
  }

  private void refreshSequence() {
    final Map<String, Long> map = sequenceFactory.refreshSeq();
    currentValue = map.get("init");
    maxValue = map.get("maxValue");
  }
}
