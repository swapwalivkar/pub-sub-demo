package com.example.flexible.pubsub;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;

public class PubSubSubcription {
	
	
	
	public List<ReceivedMessage> receiveMessageSynchronous(String projectId, String subscriptionId) {
		try {
		SubscriberStubSettings subscriberStubSettings =
			    SubscriberStubSettings.newBuilder()
			        .setTransportChannelProvider(
			            SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
			                .setMaxInboundMessageSize(20 << 20) // 20MB
			                .build())
			        .build();

			try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
			  // String projectId = "my-project-id";
			  // String subscriptionId = "my-subscription-id";
			  // int numOfMessages = 10;   // max number of messages to be pulled
			  String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
			  PullRequest pullRequest =
			      PullRequest.newBuilder()
			          .setMaxMessages(10)
			          .setReturnImmediately(false) // return immediately if messages are not available
			          .setSubscription(subscriptionName)
			          .build();

			  // use pullCallable().futureCall to asynchronously perform this operation
			  PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
			  List<String> ackIds = new ArrayList<>();
			  for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
			    // handle received message
			    // ...
			    ackIds.add(message.getAckId());
			  }
			  // acknowledge received messages
			  AcknowledgeRequest acknowledgeRequest =
			      AcknowledgeRequest.newBuilder()
			          .setSubscription(subscriptionName)
			          .addAllAckIds(ackIds)
			          .build();
			  // use acknowledgeCallable().futureCall to asynchronously perform this operation
			  subscriber.acknowledgeCallable().call(acknowledgeRequest);
			  return pullResponse.getReceivedMessagesList();
			}
		}catch(Exception ex) {
			System.out.println(ex.getMessage());
		}
		return null;
	}
}
