package com.yongpoliu.sequence;

public class SequenceException extends RuntimeException {

  public SequenceException(String seqName, Throwable t) {
    super(seqName, t);
  }
}
