package io.advantageous.reakt.kinesis;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.*;
import io.advantageous.reakt.promise.Promise;

import static io.advantageous.reakt.promise.Promises.invokablePromise;

public class KinesisClient {


    private final AmazonKinesisAsyncClient amazonKinesisAsyncClient;

    public KinesisClient(AmazonKinesisAsyncClient amazonKinesisAsyncClient) {
        this.amazonKinesisAsyncClient = amazonKinesisAsyncClient;
    }

    public static KinesisClient create(final AmazonKinesisAsyncClient amazonKinesisAsyncClient) {
        return new KinesisClient(amazonKinesisAsyncClient);
    }


    /**
     * Adds or updates tags for the specified Amazon Kinesis stream.
     *
     * @param request add tags to stream request
     * @return promise AddTagsToStreamResult
     */
    public Promise<AddTagsToStreamResult> addTagsToStream(final AddTagsToStreamRequest request) {
        return invokablePromise(returnPromise ->
                amazonKinesisAsyncClient.addTagsToStreamAsync(request, convertPromiseToAsyncResult(returnPromise)));

    }

    /**
     * Decreases the Amazon Kinesis stream's retention period, which is the length of time data records
     * are accessible after they are added to the stream.
     * @param request request
     * @return promise of DecreaseStreamRetentionPeriodResult
     */
    public Promise<DecreaseStreamRetentionPeriodResult> decreaseStreamRetentionPeriod(
            final DecreaseStreamRetentionPeriodRequest request) {
        return invokablePromise(returnPromise -> amazonKinesisAsyncClient.decreaseStreamRetentionPeriodAsync(request,
                convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Deletes an Amazon Kinesis stream and all its shards and data.
     * @param request request
     * @return promise of DeleteStreamResult
     */
    public Promise<DeleteStreamResult>	deleteStream(final DeleteStreamRequest request) {
        return invokablePromise(returnPromise-> amazonKinesisAsyncClient.deleteStreamAsync(request,
                convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Deletes an Amazon Kinesis stream and all its shards and data.
     * @param streamName stream name
     * @return promise of DeleteStreamResult
     */
    public Promise<DeleteStreamResult>	deleteStream(final String streamName) {
        return invokablePromise(returnPromise-> amazonKinesisAsyncClient.deleteStreamAsync(streamName,
                convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     *
     * @param request request
     * @return promise of DescribeStreamResult
     */
    public Promise<DescribeStreamResult> describeStream(final DescribeStreamRequest request) {
        return invokablePromise(promise->amazonKinesisAsyncClient.describeStreamAsync(request,
                convertPromiseToAsyncResult(promise)));
    }

    /**
     * Creates an Amazon Kinesis stream.
     *
     * @param request create stream request
     * @return Promise of CreateStreamResult.
     */
    public Promise<CreateStreamResult> createStream(final CreateStreamRequest request) {
        return invokablePromise(returnPromise ->
                amazonKinesisAsyncClient.createStreamAsync(request, convertPromiseToAsyncResult(returnPromise)));
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
