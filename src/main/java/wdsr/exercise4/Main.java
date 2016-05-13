package wdsr.exercise4;

import wdsr.exercise4.consumer.JmsConsumer;

public class Main {
	public static void main(String[] args) {
		JmsConsumer jmsSender = new JmsConsumer("DonLeo93.QUEUE");
		jmsSender.registerCallback();
	}
}
