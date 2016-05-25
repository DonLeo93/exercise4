package wdsr.exercise4.publisher;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPublisher {
	private static final Logger log = LoggerFactory.getLogger(JmsPublisher.class);
	
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory = null;
	
	public JmsPublisher(final String topicName) {
		this.topicName = topicName;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	}
	
	public void sendTopic() {
		try(Connection connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);){
			connection.start();				
			Topic topic = session.createTopic(topicName);
			MessageProducer producer = session.createProducer(topic);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			
			TextMessage message = null;
			long startTime = System.currentTimeMillis();
			for(int i=0; i<10000; i++){
				message = session.createTextMessage("test_"+i);
				producer.send(message);
			}
			long endTime = System.currentTimeMillis()-startTime;
			log.info("10000 persistent messages sent in {} milliseconds.",endTime);
			
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			startTime = System.currentTimeMillis();
			for(int i=10000; i<20000; i++){
				message = session.createTextMessage("test_"+i);
				producer.send(message);
			}
			endTime = System.currentTimeMillis()-startTime;
			log.info("10000 non-persistent messages sent in {} milliseconds.",endTime);
		} catch (JMSException e){
			log.error("Error durring sending topic: ", e);
		}
	}
}