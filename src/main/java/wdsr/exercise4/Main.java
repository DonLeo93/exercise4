package wdsr.exercise4;

import wdsr.exercise4.publisher.JmsPublisher;

public class Main {
	public static void main(String[] args) {
		JmsPublisher jmsSender = new JmsPublisher("DonLeo93.TOPIC");
		jmsSender.sendTopic();
	}
}
