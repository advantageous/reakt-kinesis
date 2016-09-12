package io.advantageous.reakt.kinesis.impl;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.*;
import io.advantageous.reakt.kinesis.KinesisClient;
import io.advantageous.reakt.promise.Promise;

import java.nio.ByteBuffer;

import static io.advantageous.reakt.promise.Promises.invokablePromise;

public class KinesisClientImpl implements KinesisClient {


    private final AmazonKinesisAsyncClient amazonKinesisAsyncClient;

    public KinesisClientImpl(AmazonKinesisAsyncClient amazonKinesisAsyncClient) {
        this.amazonKinesisAsyncClient = amazonKinesisAsyncClient;
    }


    /**
     * Adds or updates tags for the specified Amazon Kinesis stream.
     *
     * @param request add tags to stream request
     * @return promise AddTagsToStreamResult
     */
    @Override
    public Promise<AddTagsToStreamResult> addTagsToStream(final AddTagsToStreamRequest request) {
        return invokablePromise(returnPromise ->
                amazonKinesisAsyncClient.addTagsToStreamAsync(request, convertPromiseToAsyncResult(returnPromise)));

    }


    /**
     * Creates an Amazon Kinesis stream.
     *
     * @param request create stream request
     * @return Promise of CreateStreamResult.
     */
    @Override
    public Promise<CreateStreamResult> createStream(final CreateStreamRequest request) {
        return invokablePromise(returnPromise ->
                amazonKinesisAsyncClient.createStreamAsync(request, convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Decreases the Amazon Kinesis stream's retention period, which is the length of time data records
     * are accessible after they are added to the stream.
     *
     * @param request request
     * @return promise of DecreaseStreamRetentionPeriodResult
     */
    @Override
    public Promise<DecreaseStreamRetentionPeriodResult> decreaseStreamRetentionPeriod(
            final DecreaseStreamRetentionPeriodRequest request) {
        return invokablePromise(returnPromise -> amazonKinesisAsyncClient.decreaseStreamRetentionPeriodAsync(request,
                convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Deletes an Amazon Kinesis stream and all its shards and data.
     *
     * @param request request
     * @return promise of DeleteStreamResult
     */
    @Override
    public Promise<DeleteStreamResult> deleteStream(final DeleteStreamRequest request) {
        return invokablePromise(returnPromise -> amazonKinesisAsyncClient.deleteStreamAsync(request,
                convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Deletes an Amazon Kinesis stream and all its shards and data.
     *
     * @param streamName stream name
     * @return promise of DeleteStreamResult
     */
    @Override
    public Promise<DeleteStreamResult> deleteStream(final String streamName) {
        return invokablePromise(returnPromise -> amazonKinesisAsyncClient.deleteStreamAsync(streamName,
                convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * @param request request
     * @return promise of DescribeStreamResult
     */
    @Override
    public Promise<DescribeStreamResult> describeStream(final DescribeStreamRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.describeStreamAsync(request,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * @param streamName streamName
     * @return promise of DescribeStreamResult
     */
    @Override
    public Promise<DescribeStreamResult> describeStream(final String streamName) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.describeStreamAsync(streamName,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * @param streamName            streamName
     * @param limit                 limit
     * @param exclusiveStartShardId exclusiveStartShardId
     * @return promise of DescribeStreamResult
     */
    @Override
    public Promise<DescribeStreamResult> describeStream(final String streamName, final int limit,
                                                        final String exclusiveStartShardId) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.describeStreamAsync(streamName, limit,
                exclusiveStartShardId,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Disables enhanced monitoring.
     *
     * @param request request
     * @return promise of DisableEnhancedMonitoringResult
     */
    @Override
    public Promise<DisableEnhancedMonitoringResult> disableEnhancedMonitoring(
            final DisableEnhancedMonitoringRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.disableEnhancedMonitoringAsync(request,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Enables enhanced Amazon Kinesis stream monitoring for shard-level metrics.
     *
     * @param request request
     * @return promise of EnableEnhancedMonitoringResult
     */
    @Override
    public Promise<EnableEnhancedMonitoringResult> enableEnhancedMonitoring(
            final EnableEnhancedMonitoringRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.enableEnhancedMonitoringAsync(request,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Gets data records from an Amazon Kinesis stream's shard.
     *
     * @param request request
     * @return promise of GetRecordsResult
     */
    @Override
    public Promise<GetRecordsResult> getRecords(final GetRecordsRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.getRecordsAsync(request,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Gets an Amazon Kinesis shard iterator.
     *
     * @param request request
     * @return promise of GetShardIteratorResult
     */
    @Override
    public Promise<GetShardIteratorResult> getShardIteratorAsync(final GetShardIteratorRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.getShardIteratorAsync(request,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Simplified method form for invoking the GetShardIterator operation.
     *
     * @param streamName        stream name
     * @param shardId           shard id
     * @param shardIteratorType shart iterator type
     * @return promise of GetShardIteratorResult
     */
    @Override
    public Promise<GetShardIteratorResult> getShardIteratorAsync(final String streamName, final String shardId,
                                                                 final String shardIteratorType) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.getShardIteratorAsync(streamName, shardId,
                shardIteratorType,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the GetShardIterator operation.
     *
     * @param streamName             stream name
     * @param shardId                shard id
     * @param shardIteratorType      shart iterator type
     * @param startingSequenceNumber starting sequence number
     * @return promise of GetShardIteratorResult
     */
    @Override
    public Promise<GetShardIteratorResult> getShardIteratorAsync(final String streamName, final String shardId,
                                                                 final String shardIteratorType,
                                                                 final String startingSequenceNumber) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.getShardIteratorAsync(streamName, shardId,
                shardIteratorType, startingSequenceNumber,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Increases stream's retention period. Retention period is the length of time data records are accessible
     * after they are added to the stream.
     *
     * @param request request
     * @return promise of IncreaseStreamRetentionPeriodResult
     */
    @Override
    public Promise<IncreaseStreamRetentionPeriodResult> increaseStreamRetentionPeriod(
            final IncreaseStreamRetentionPeriodRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.increaseStreamRetentionPeriodAsync(request,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the ListStreams operation.
     *
     * @return promise of ListStreamsResult
     */
    @Override
    public Promise<ListStreamsResult> listStreams() {
        return invokablePromise(promise -> amazonKinesisAsyncClient.listStreamsAsync(
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the ListStreams operation.
     *
     * @param limit                    limit
     * @param exclusiveStartStreamName exclusiveStartStreamName
     * @return promise of ListStreamsResult
     */
    @Override
    public Promise<ListStreamsResult> listStreams(final int limit, final String exclusiveStartStreamName) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.listStreamsAsync(limit, exclusiveStartStreamName,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the ListStreams operation.
     *
     * @param exclusiveStartStreamName exclusiveStartStreamName
     * @return promise of ListStreamsResult
     */
    @Override
    public Promise<ListStreamsResult> listStreams(final String exclusiveStartStreamName) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.listStreamsAsync(exclusiveStartStreamName,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the ListStreams operation.
     *
     * @return promise of ListStreamsResult
     */
    @Override
    public Promise<ListStreamsResult> listStreams(final ListStreamsRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.listStreamsAsync(request,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Lists the tags for the specified Amazon Kinesis stream.
     *
     * @param request request
     * @return promise of ListTagsForStreamResult
     */
    @Override
    public Promise<ListTagsForStreamResult> listTagsForStream(final ListTagsForStreamRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.listTagsForStreamAsync(request,
                convertPromiseToAsyncResult(promise)));
    }


    /**
     * Merges two adjacent shards. Combines shards into a single shard.
     * Reduce the stream's capacity to ingest and transport data.
     *
     * @param request request
     * @return promise of MergeShardsResult
     */
    @Override
    public Promise<MergeShardsResult> mergeShards(final MergeShardsRequest request) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.mergeShardsAsync(request,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Simplified method form for invoking the MergeShards operation.
     *
     * @param streamName           stream name
     * @param shardToMerge         shard to merge
     * @param adjacentShardToMerge adjacent shard to merge
     * @return promise of MergeShardsResult
     */
    @Override
    public Promise<MergeShardsResult> mergeShards(final String streamName, final String shardToMerge,
                                                  final String adjacentShardToMerge) {
        return invokablePromise(promise -> amazonKinesisAsyncClient.mergeShardsAsync(streamName, shardToMerge,
                adjacentShardToMerge, convertPromiseToAsyncResult(promise)));
    }

    /**
     * Writes a single data record into an Amazon Kinesis stream.
     *
     * @param request request
     * @return promise of PutRecordResult
     */
    @Override
    public Promise<PutRecordResult> putRecord(final PutRecordRequest request) {
        return invokablePromise(p -> amazonKinesisAsyncClient.putRecordAsync(request, convertPromiseToAsyncResult(p)));
    }


    /**
     * Simplified method form for invoking the PutRecord operation.
     *
     * @param streamName   stream name
     * @param data         data buffer
     * @param partitionKey partition key
     * @return promise of PutRecordResult
     */
    @Override
    public Promise<PutRecordResult> putRecord(final String streamName, final ByteBuffer data,
                                              final String partitionKey) {
        return invokablePromise(p -> amazonKinesisAsyncClient.putRecordAsync(streamName, data, partitionKey,
                convertPromiseToAsyncResult(p)));
    }


    /**
     * Simplified method form for invoking the PutRecord operation.
     *
     * @param streamName                stream name
     * @param data                      data buffer
     * @param partitionKey              partition key
     * @param sequenceNumberForOrdering sequenceNumberForOrdering
     * @return promise of PutRecordResult
     */
    @Override
    public Promise<PutRecordResult> putRecord(final String streamName, final ByteBuffer data,
                                              final String partitionKey, final String sequenceNumberForOrdering) {
        return invokablePromise(p -> amazonKinesisAsyncClient.putRecordAsync(streamName, data, partitionKey,
                sequenceNumberForOrdering, convertPromiseToAsyncResult(p)));
    }

    /**
     * Writes multiple data records into a stream in a single call.
     *
     * @param request request
     * @return promise of PutRecordResult
     */
    @Override
    public Promise<PutRecordsResult> putRecords(final PutRecordsRequest request) {
        return invokablePromise(p -> amazonKinesisAsyncClient.putRecordsAsync(request, convertPromiseToAsyncResult(p)));
    }


    /**
     * Removes tags from the specified Amazon Kinesis stream.
     *
     * @param request request
     * @return promise of RemoveTagsFromStreamResult
     */
    @Override
    public Promise<RemoveTagsFromStreamResult> removeTagsFromStream(final RemoveTagsFromStreamRequest request) {
        return invokablePromise(p -> amazonKinesisAsyncClient.removeTagsFromStreamAsync(request,
                convertPromiseToAsyncResult(p)));
    }


    /**
     * Splits a shard into two new shards in the stream to increase the stream's capacity to ingest and transport data.
     *
     * @param request request
     * @return promise of SplitShardResult
     */
    @Override
    public Promise<SplitShardResult> splitShardAsync(final SplitShardRequest request) {
        return invokablePromise(p -> amazonKinesisAsyncClient.splitShardAsync(request,
                convertPromiseToAsyncResult(p)));
    }


    /**
     * Splits a shard into two new shards in the stream to increase the stream's capacity to ingest and transport data.
     *
     * @param streamName         stream name
     * @param shardToSplit       shard to split
     * @param newStartingHashKey starting hash key
     * @return promise of SplitShardResult
     */
    @Override
    public Promise<SplitShardResult> splitShardAsync(final String streamName, final String shardToSplit,
                                                     final String newStartingHashKey) {
        return invokablePromise(p -> amazonKinesisAsyncClient.splitShardAsync(streamName, shardToSplit,
                newStartingHashKey, convertPromiseToAsyncResult(p)));
    }


    @Override
    public AmazonKinesisAsyncClient getAmazonKinesisAsyncClient() {
        return amazonKinesisAsyncClient;
    }

    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> AsyncHandler<REQUEST, RESPONSE>
    convertPromiseToAsyncResult(final Promise<RESPONSE> returnPromise) {
        return new AsyncHandler<REQUEST, RESPONSE>() {
            @Override
            public void onError(Exception exception) {
                returnPromise.reject(exception);
            }

            @Override
            public void onSuccess(REQUEST request, RESPONSE response) {
                returnPromise.resolve(response);
            }
        };
    }
}
