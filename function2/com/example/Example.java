package com.example;

import com.example.Example.PubSubMessage;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.StringTokenizer;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.*;
import java.io.UnsupportedEncodingException;

public class Example implements BackgroundFunction<PubSubMessage>{

    private final String projectId = "qwiklabs-gcp-03-b9862b43147b";
    private final String topicId = "topic2";

    public String targetBucketName = "";
    public String fileName = "";

    private static final Logger logger = Logger.getLogger(Example.class.getName());

    @Override
    public void accept(PubSubMessage message, Context context) throws Exception {
        String data = message.data != null
                ? new String(Base64.getDecoder().decode(message.data))
                : "Hello, World";
        logger.info(data);

        StringTokenizer st = new StringTokenizer(data,"/");
        targetBucketName = st.nextToken();
        fileName = st.nextToken();

        StringBuffer sb = new StringBuffer();
        List<String> messages = new ArrayList<String>();
        String[] datas = getBlobData(projectId, targetBucketName, fileName);
        for(String row : datas){
            messages.add(row);
            System.out.println(String.format("'%s'%n", row));
        }
        System.out.println("Topic2 Call Pub/Sub");
        publishWithErrorHandlerExample(projectId, topicId, messages);
    }

    public static class PubSubMessage {
        String data;
        Map<String, String> attributes;
        String messageId;
        String publishTime;
    }

    public String[] getBlobData(String projectId, String bucketName, String blobName) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);
        byte[] content = blob.getContent();
        try {
            return new String(content, "UTF-8").split("\n", 0);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new String[0];
    }

    public static void publishWithErrorHandlerExample(String projectId, String topicId, List<String> messages)
            throws IOException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            for (final String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {

                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException) {
                                    ApiException apiException = ((ApiException) throwable);
                                    // details on the API exception
                                    System.out.println(apiException.getStatusCode().getCode());
                                    System.out.println(apiException.isRetryable());
                                }
                                System.out.println("Error publishing message : " + message);
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                // Once published, returns server-assigned message ids (unique within the topic)
                                System.out.println("Published message ID: " + messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}
