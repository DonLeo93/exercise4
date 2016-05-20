package wdsr.exercise4.consumer;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsConsumer {
	private static final Logger log = LoggerFactory.getLogger(JmsConsumer.class);
	
	private ActiveMQConnectionFactory connectionFactory=null;
	private MessageConsumer consumer = null;
	private int counter=0;
	private String queueName;

	public JmsConsumer(final String queueName) {
		try{
			this.queueName=queueName;
			connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connectionFactory.setTrustAllPackages(true);  
		}catch(Exception e){
			log.error("Error: ",e);
		}
	}

	public void registerCallback() {
			try(Connection connection = connectionFactory.createConnection();
				Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);) {
				connection.start();
				Destination destination = session.createQueue(queueName);
				consumer = session.createConsumer(destination);
				
				consumer.setMessageListener( message -> {
					if(message instanceof TextMessage){
						try {
							counter++;
							log.info("#{} - MSG: {}", counter, ((TextMessage) message).getText());
						} catch (Exception e) {
							log.error("Error: ", e);
						}
					}
				});
			} catch (JMSException e) {
				log.error("Error: ", e);
			}
	}
	
	public void shutdown(){
		try{
			log.info("Total messages: {}",counter);
			if(consumer!=null)
				consumer.close();
		}catch(JMSException e){
			log.error("Error: ",e);
		}
	}
}
