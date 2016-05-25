package wdsr.exercise4.consumer;

import javax.jms.Connection;
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
	private Connection connection=null;
	private Session session=null;
	private int counter=0;

	public JmsConsumer(final String queueName) {
		try{
			connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connectionFactory.setTrustAllPackages(true); 
			connection = connectionFactory.createConnection();
			session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
			
			Destination destination = session.createQueue(queueName);
			consumer = session.createConsumer(destination);
		}catch(Exception e){
			log.error("Error durring creating connection: ",e);
		}
	}

	public void registerCallback() {
			try{
				consumer.setMessageListener( message -> {
					if(message instanceof TextMessage){
						try {
							counter++;
							log.info("#{} - MSG: {}", counter, ((TextMessage) message).getText());
						} catch (Exception e) {
							log.error("Error durring loging information about message: ", e);
						}
					}
				});
				connection.start();
			} catch (JMSException e) {
				log.error("Error durring registering callback: ", e);
			}
	}
	
	public void shutdown(){
		try{
			log.info("Total messages: {}",counter);
			if(consumer!=null)
				consumer.close();
		}catch(JMSException e){
			log.error("Error durring closing consumer: ",e);
		}
	}
}
