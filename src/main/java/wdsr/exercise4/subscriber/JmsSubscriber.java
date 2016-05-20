package wdsr.exercise4.subscriber;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSubscriber {
	private static final Logger log = LoggerFactory.getLogger(JmsSubscriber.class);
	
	private Connection connection = null;
	private Session session = null;
	private TopicSubscriber subscriber = null;
	private int counter = 0;
	
	public JmsSubscriber(final String topicName) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connection = connectionFactory.createConnection();
			connection.setClientID("1");
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic(topicName);
			subscriber = session.createDurableSubscriber(topic, "subscriber");
		} catch (Exception e) {
			log.error("Error: ", e);
		}
	}
	
	public void getMessage() {
		try {
			subscriber.setMessageListener( message -> {
				if(message instanceof TextMessage){
					try {
						counter++;
						log.info("# {} MSG: {}", counter, ((TextMessage) message).getText());
					} catch (Exception e) {
						log.error("Error: ", e);
					}
				}				
			});
		} catch (JMSException e) {
			log.error("Error: ", e);
		}
	}
}