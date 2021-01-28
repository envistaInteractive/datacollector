package com.streamsets.pipeline.stage.origin.mongodb.changestream;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBUtil;
import com.streamsets.pipeline.stage.origin.mongodb.AbstractMongoDBSource;
import com.streamsets.pipeline.stage.origin.mongodb.MongoSourceConfigBean;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoDBChangeStreamSource extends AbstractMongoDBSource {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBChangeStreamSource.class);
    static final String OP_TYPE_FIELD = "op";
    static final String NS_FIELD = "ns";

    private final MongoDBChangeStreamSourceConfigBean mongoDBChangeStreamSourceConfigBean;

    private String lastResumeToken;

    private boolean checkBatchSize = true;

    public MongoDBChangeStreamSource(MongoSourceConfigBean configBean, MongoDBChangeStreamSourceConfigBean
            mongoDBChangeStreamSourceConfigBean) {
        super(configBean);
        this.mongoDBChangeStreamSourceConfigBean = mongoDBChangeStreamSourceConfigBean;
    }

    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();

        if (issues.isEmpty()) {
            lastResumeToken = mongoDBChangeStreamSourceConfigBean.initialResumeToken;
        }
        return issues;
    }

    @Override
    public String produce(String lastSourceToken, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        int batchSize = Math.min(maxBatchSize, configBean.batchSize);
        if (!getContext().isPreview() && checkBatchSize && configBean.batchSize > maxBatchSize) {
            getContext().reportError(Errors.MONGODB_43, maxBatchSize);
            checkBatchSize = false;
        }

        int numContiguousErrors = 0;
        long batchWaitTime = System.currentTimeMillis() + (configBean.maxBatchWaitTime * 1000);
        int numOfRecordsProduced = 0;
        initStateIfNeeded(lastSourceToken, batchSize);
        while (numOfRecordsProduced < batchSize) {
            try {
                Record record = getChangeStreamRecord();
                numContiguousErrors = 0;
                if (record != null) {
                    batchMaker.addRecord(record);
                    numOfRecordsProduced++;
                } else if (breakOrWaitIfNeeded(batchWaitTime - System.currentTimeMillis())) {
                    break;
                }
            } catch (MongoCursorNotFoundException e) {
                numContiguousErrors++;
                LOG.error("Cursor not found, reinitializing cursor...", e);
                initStateIfNeeded(lastResumeToken, batchSize);
            } catch (MongoException e) {
                numContiguousErrors++;
                if (StringUtils.containsIgnoreCase(e.getMessage(), "Cursor has been closed")) {
                    LOG.error("Cursor has been closed, reinitializing cursor...", e);
                    initStateIfNeeded(lastResumeToken, batchSize);
                }
                else {
                    LOG.error("MongoError while getting Change Stream Record", e);
                    initStateIfNeeded(lastResumeToken, batchSize);
                    // errorRecordHandler.onError(Errors.MONGODB_10, e.toString(), e);
                }
            } catch (IOException | IllegalArgumentException e) {
                numContiguousErrors++;
                LOG.error("Error while getting Change Stream Record, reinitializing cursor...", e);
                initStateIfNeeded(lastResumeToken, batchSize);
                // errorRecordHandler.onError(Errors.MONGODB_10, e.toString(), e);
            }
            if (numContiguousErrors >= 100) {
                throw new RuntimeException("Too many continuous errors > 100 when trying to re-initialize the change " +
                    "stream");
            }
        }
        return createOffset();
    }

    private boolean breakOrWaitIfNeeded(long remainingWaitTime) {
        //Wait the remaining time if there is no record
        //and try for last time before to read records and then return whatever was read
        if (remainingWaitTime > 0) {
            ThreadUtil.sleep(remainingWaitTime);
            //Don't break and try reading now
            return false;
        }
        //break
        return true;
    }

    private boolean shouldInitOffset(String lastToken) {
        return !StringUtils.isEmpty(lastToken);
    }

    private void initStateIfNeeded(String lastToken, int batchSize) {
        try {
            if (shouldInitOffset(lastToken)) {
                LOG.info("Offset initialization required. Last Token: {}", lastToken);
                lastResumeToken = lastToken;
            }
            if (cursor == null) {
                LOG.info("Cursor null while initializing state. Last Token: {} Batch Size: {}", lastToken, batchSize);
                try {
                  prepareCursor(
                      lastResumeToken,
                      mongoDBChangeStreamSourceConfigBean.filterChangeStreamOpTypes,
                      mongoDBChangeStreamSourceConfigBean.fullDocument,
                      batchSize
                  );
                }
                catch (Exception e) {
                  LOG.error("Error while preparing cursor with last resume token " + lastResumeToken + ", hence " +
                      "resetting origin", e);
                  prepareCursor(
                      "",
                      mongoDBChangeStreamSourceConfigBean.filterChangeStreamOpTypes,
                      mongoDBChangeStreamSourceConfigBean.fullDocument,
                      batchSize
                  );
                }
            }
        }
        catch (DecoderException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void prepareCursor(String lastToken, List<ChangeStreamOpType> filterChangeStreamTypes,
                               boolean fullDocument, int batchSize) throws DecoderException {
        LOG.info("Getting new cursor with Resume Token : '{}' and Batch Size : '{}'",
                lastToken, batchSize);

        List<Bson> pipeline = new ArrayList<>();
        BsonDocument lastTokenDoc = null;
        if (StringUtils.isNotBlank(lastToken)) {
            byte[] lastTokenBytes = Hex.decodeHex(lastToken.toCharArray());
            lastTokenDoc = new BsonDocument("_data", new BsonBinary(lastTokenBytes));
        }

        if (CollectionUtils.isNotEmpty(filterChangeStreamTypes)) {
            pipeline.add(Aggregates.match(Filters.in("operationType",
                    filterChangeStreamTypes.stream().map(ChangeStreamOpType::getOp).toArray(String[]::new))));
        }

        ChangeStreamIterable watcher = mongoCollection.watch(pipeline);

        if (fullDocument) {
            watcher.fullDocument(FullDocument.UPDATE_LOOKUP);
        }

        if (lastTokenDoc != null) {
            watcher.resumeAfter(lastTokenDoc);
        }

        cursor = watcher.cursor();
    }

    private String createOffset() {
        LOG.info("Creating offset using last resume token: {}", lastResumeToken);
        return lastResumeToken;
    }

    private static void populateGenericOperationTypeInHeader(Record record, String opType) {
        ChangeStreamOpType changeStreamOpType = ChangeStreamOpType.getChangeStreamTypeFromOpString(opType);
        if (changeStreamOpType == null) {
            throw new IllegalArgumentException(Utils.format("Unsupported Op Log Op type : {}", opType));
        }
        int operationType;
        switch (changeStreamOpType) {
            case INSERT:
                operationType = OperationType.INSERT_CODE;
                break;
            case UPDATE:
                operationType = OperationType.UPDATE_CODE;
                break;
            case DELETE:
                operationType = OperationType.DELETE_CODE;
                break;
            default:
                throw new IllegalArgumentException(Utils.format("Unsupported Op Log Op type : {}", opType));
        }
        record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationType));
    }

    private Record getChangeStreamRecord() throws IOException {
        Record record = null;
        ChangeStreamDocument<Document> changeStreamDoc = (ChangeStreamDocument<Document>) cursor.tryNext();
        if (changeStreamDoc != null) {
            lastResumeToken = Hex.encodeHexString(changeStreamDoc.getResumeToken().getBinary("_data").getData());
            if (LOG.isInfoEnabled()) {
                LOG.info("Last Resume Token: {}", lastResumeToken);
            }

            String documentKey = null;
            if (changeStreamDoc.getDocumentKey() != null) {
                if (changeStreamDoc.getDocumentKey().get("_id").getBsonType().equals(BsonType.OBJECT_ID)) {
                    documentKey = changeStreamDoc.getDocumentKey().getObjectId("_id").getValue().toString();
                } else if (changeStreamDoc.getDocumentKey().get("_id").getBsonType().equals(BsonType.STRING)) {
                    documentKey = changeStreamDoc.getDocumentKey().getString("_id").getValue();
                }
            }

            record = getContext().createRecord(
                    MongoDBUtil.getSourceRecordId(
                            configBean.mongoConfig.connectionString,
                            configBean.mongoConfig.database,
                            configBean.mongoConfig.collection,
                            documentKey + "::" + createOffset()
                    )
            );
            String ns = changeStreamDoc.getNamespace() != null ? changeStreamDoc.getNamespace().getFullName() :
                    null;
            String opType = changeStreamDoc.getOperationType().getValue();
            record.getHeader().setAttribute(NS_FIELD, ns);
            record.getHeader().setAttribute(OP_TYPE_FIELD, opType);
            //Populate Generic operation type
            populateGenericOperationTypeInHeader(record, opType);

            Document doc = changeStreamDoc.getFullDocument();
            if (doc != null) {
                record.set(Field.create(MongoDBUtil.createFieldFromDocument(doc)));
            }
        } else {
            LOG.trace("Document from Cursor is null, No More Records");
        }
        return record;
    }
}
