package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class ChangeStreamOpTypeChooserValues extends BaseEnumChooserValues<ChangeStreamOpType>{
  public ChangeStreamOpTypeChooserValues() {
    super(ChangeStreamOpType.class);
  }
}
