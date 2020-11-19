package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.stage.origin.mongodb.MongoDBDSource;

@StageDef(
        version = 1,
        label = "MongoDB Change Stream",
        description = "Reads Change Stream records from MongoDB",
        icon = "mongodb.png",
        execution = ExecutionMode.STANDALONE,
        recordsByRef = true,
        onlineHelpRefUrl = "index.html?contextID=task_qj5_drw_4y",
        upgrader = MongoDBChangeStreamSourceUpgrader.class,
        upgraderDef = "upgrader/MongoDBChangeStreamDSource.yaml",
        resetOffset = true
)
@GenerateResourceBundle
@HideConfigs(
        {
                "configBean.initialOffset",
                "configBean.offsetField",
                "configBean.offsetType"
        }
)
public class MongoDBChangeStreamDSource extends MongoDBDSource {

    @ConfigDefBean()
    public MongoDBChangeStreamSourceConfigBean mongoDBChangeStreamSourceConfigBean;

    @Override
    protected Source createSource() {
        return new MongoDBChangeStreamSource(configBean, mongoDBChangeStreamSourceConfigBean);
    }
}
