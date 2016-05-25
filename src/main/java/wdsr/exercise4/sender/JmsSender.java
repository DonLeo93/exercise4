package wdsr.exercise4.sender;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private ActiveMQConnectionFactory connectionFactory=null;

	public JmsSender(final String queueName) {
		this.queueName = queueName;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	}

	public void sendTextToQueue() {
		try(Connection connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);){
			connection.start();
			Destination destination = session.createQueue(queueName);
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			
			TextMessage message = null;
			long startTime = System.currentTimeMillis();
			for(int i = 0; i<10000; i++){
				message = session.createTextMessage("test_"+i);
				producer.send(message);
			}
			long endTime = System.currentTimeMillis()-startTime;
			log.info("10000 non-persistent messages sent in {} milliseconds.",endTime);
			
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			startTime = System.currentTimeMillis();
			for(int i = 10000; i<20000; i++){
				message = session.createTextMessage("test_"+i);				
				producer.send(message);
			}
			endTime = System.currentTimeMillis()-startTime;
			log.info("10000 persistent messages sent in {} milliseconds.",endTime);
		}catch(JMSException e){
			log.error("Error durring sending text to queue: ",e);
		}
	}
}