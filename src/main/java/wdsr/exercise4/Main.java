package wdsr.exercise4;

import wdsr.exercise4.publisher.JmsPublisher;

public class Main {
	public static void main(String[] args) {
		JmsPublisher jmsPublisher = new JmsPublisher("DonLeo93.TOPIC");
		jmsPublisher.sendTopic();
	}
}
