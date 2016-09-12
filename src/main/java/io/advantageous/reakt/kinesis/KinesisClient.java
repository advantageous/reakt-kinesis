package io.advantageous.reakt.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.*;
import io.advantageous.reakt.kinesis.impl.KinesisClientImpl;
import io.advantageous.reakt.promise.Promise;

import java.nio.ByteBuffer;

public interface KinesisClient {
    static KinesisClientImpl create(AmazonKinesisAsyncClient amazonKinesisAsyncClient) {
        return new KinesisClientImpl(amazonKinesisAsyncClient);
    }

    Promise<AddTagsToStreamResult> addTagsToStream(AddTagsToStreamRequest request);

    Promise<CreateStreamResult> createStream(CreateStreamRequest request);

    Promise<DecreaseStreamRetentionPeriodResult> decreaseStreamRetentionPeriod(
            DecreaseStreamRetentionPeriodRequest request);

    Promise<DeleteStreamResult> deleteStream(DeleteStreamRequest request);

    Promise<DeleteStreamResult> deleteStream(String streamName);

    Promise<DescribeStreamResult> describeStream(DescribeStreamRequest request);

    Promise<DescribeStreamResult> describeStream(String streamName);

    Promise<DescribeStreamResult> describeStream(String streamName, int limit,
                                                 String exclusiveStartShardId);

    Promise<DisableEnhancedMonitoringResult> disableEnhancedMonitoring(
            DisableEnhancedMonitoringRequest request);

    Promise<EnableEnhancedMonitoringResult> enableEnhancedMonitoring(
            EnableEnhancedMonitoringRequest request);

    Promise<GetRecordsResult> getRecords(GetRecordsRequest request);

    Promise<GetShardIteratorResult> getShardIteratorAsync(GetShardIteratorRequest request);

    Promise<GetShardIteratorResult> getShardIteratorAsync(String streamName, String shardId,
                                                          String shardIteratorType);

    Promise<GetShardIteratorResult> getShardIteratorAsync(String streamName, String shardId,
                                                          String shardIteratorType,
                                                          String startingSequenceNumber);

    Promise<IncreaseStreamRetentionPeriodResult> increaseStreamRetentionPeriod(
            IncreaseStreamRetentionPeriodRequest request);

    Promise<ListStreamsResult> listStreams();

    Promise<ListStreamsResult> listStreams(int limit, String exclusiveStartStreamName);

    Promise<ListStreamsResult> listStreams(String exclusiveStartStreamName);

    Promise<ListStreamsResult> listStreams(ListStreamsRequest request);

    Promise<ListTagsForStreamResult> listTagsForStream(ListTagsForStreamRequest request);

    Promise<MergeShardsResult> mergeShards(MergeShardsRequest request);

    Promise<MergeShardsResult> mergeShards(String streamName, String shardToMerge,
                                           String adjacentShardToMerge);

    Promise<PutRecordResult> putRecord(PutRecordRequest request);

    Promise<PutRecordResult> putRecord(String streamName, ByteBuffer data,
                                       String partitionKey);

    Promise<PutRecordResult> putRecord(String streamName, ByteBuffer data,
                                       String partitionKey, String sequenceNumberForOrdering);

    Promise<PutRecordsResult> putRecords(PutRecordsRequest request);

    Promise<RemoveTagsFromStreamResult> removeTagsFromStream(RemoveTagsFromStreamRequest request);

    Promise<SplitShardResult> splitShardAsync(SplitShardRequest request);

    Promise<SplitShardResult> splitShardAsync(String streamName, String shardToSplit,
                                              String newStartingHashKey);

    AmazonKinesisAsyncClient getAmazonKinesisAsyncClient();
}
