package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.Stage;

import java.util.List;

public class MongoDBChangeStreamSourceConfigBean {
    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Resume Token",
            description = "Specify the change stream token to resume after. Leave it blank to opt out and resume " +
                    "notifications starting after the operation specified in the token.",
            displayPosition = 1003,
            group = "MONGODB"
    )
    public String initialResumeToken;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Full Document",
            description = "Specifying false streams only the delta of those fields modified by an update operation, " +
                    "instead of the entire updated document",
            displayPosition = 1004,
            group = "MONGODB"
    )
    public boolean fullDocument;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            label = "Operation Types",
            defaultValue = "[\"INSERT\", \"UPDATE\"]",
            description = "Change Stream Operation types to read",
            displayPosition = 1005,
            group = "MONGODB"
    )
    @MultiValueChooserModel(ChangeStreamOpTypeChooserValues.class)
    public List<ChangeStreamOpType> filterChangeStreamOpTypes;

    public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    }
}
