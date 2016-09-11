package io.advantageous.reakt.kinesis;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
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
