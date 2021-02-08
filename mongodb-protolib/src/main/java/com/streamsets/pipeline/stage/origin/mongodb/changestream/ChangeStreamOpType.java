package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.streamsets.pipeline.api.Label;

public enum ChangeStreamOpType implements Label{
  INSERT("INSERT(i)", "insert"),
  DELETE("DELETE(d)", "delete"),
  UPDATE("UPDATE(u)", "update"),
  REPLACE("REPLACE(r)", "replace");

  private final String label;
  private final String op;

  ChangeStreamOpType(String label, String op) {
    this.label = label;
    this.op = op;
  }

  @Override
  public String getLabel() {
    return this.label;
  }

  public String getOp() {
    return this.op;
  }

  public static ChangeStreamOpType getChangeStreamTypeFromOpString(String op) {
    for (ChangeStreamOpType changeStreamOpType : ChangeStreamOpType.values()) {
      if (changeStreamOpType.getOp().equals(op)) {
        return changeStreamOpType;
      }
    }
    return null;
  }

}
