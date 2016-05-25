package wdsr.exercise4;

import wdsr.exercise4.subscriber.JmsSubscriber;

public class Main {
	public static void main(String[] args) {
		JmsSubscriber jmsSubscriber = new JmsSubscriber("DonLeo93.TOPIC");
		jmsSubscriber.getMessage();
	}
}
