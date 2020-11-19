package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

import java.util.List;

public class MongoDBChangeStreamSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }

    return configs;
  }

  private void upgradeV1toV2(List<Config> configs) {
    configs.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "authSource", ""));
  }
}
