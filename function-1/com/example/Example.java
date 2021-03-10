package com.example;

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
import java.util.concurrent.TimeUnit;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.*;
import java.io.UnsupportedEncodingException;
import java.io.BufferedWriter;

public class Example implements HttpFunction {

    private final String projectId = "qwiklabs-gcp-03-b9862b43147b";
    private final String targetBucketName = "k-data-bucket-20210310";
    private final String topicId = "topic1";

    @Override
    public void service(HttpRequest request, HttpResponse response) throws Exception {
        response.setContentType("Content-Type: text/plain; charset=UTF-8");
        BufferedWriter writer = response.getWriter();

        String name = "";
        if (!request.getFirstQueryParameter("name").isEmpty()) {
            name = request.getFirstQueryParameter("name").get();
        }
        if(name.isEmpty()){
            writer.write("Query parameter 'name' is empty");
            System.out.println("Query parameter 'name' is empty");
            return;
        }

        String filePath = targetBucketName + "/" + name;
        String[] datas = getBlobData(projectId, targetBucketName, name);
        for(String data : datas){
            writer.write(String.format("'%s'%n", data));
            System.out.println(String.format("'%s'%n", data));
        }
        System.out.println("Call Pub/Sub");
        publishWithErrorHandlerExample(projectId, topicId, filePath);
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

    public static void publishWithErrorHandlerExample(String projectId, String topicId, String filePath)
            throws IOException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            //List<String> messages = Arrays.asList("first message", "second message");
            List<String> messages = Arrays.asList(filePath);

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