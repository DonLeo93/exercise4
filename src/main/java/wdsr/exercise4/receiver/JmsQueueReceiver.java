package wdsr.exercise4.receiver;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private Connection connection = null;
	private Session session = null;
	private MessageConsumer consumerPriceAlert = null;
	private MessageConsumer consumerVolumeAlert = null;
	private String queueName = null;
	private ActiveMQConnectionFactory connectionFactory = null;
	
	static final String PRICE_ALERT_TYPE = "PriceAlert";
	static final String VOLUME_ALERT_TYPE = "VolumeAlert";
	
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		this.queueName=queueName;
		connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
		connectionFactory.setTrustAllPackages(true);
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(this.queueName);
			consumerPriceAlert = session.createConsumer(destination, "JMSType='PriceAlert'");
			consumerVolumeAlert = session.createConsumer(destination, "JMSType='VolumeAlert'");
			connection.start();
			
			consumerPriceAlert.setMessageListener( message -> {
				try {
					if(message instanceof TextMessage){
						List<String> alert = new ArrayList<>();
						if(message.getJMSType().equals(PRICE_ALERT_TYPE)){
							String[] priceAlertMsg = ((TextMessage)message).getText().split("\n");
							
							alert.add(priceAlertMsg[0].split("=")[1]);
							alert.add(priceAlertMsg[1].split("=")[1]);
							alert.add(priceAlertMsg[2].split("=")[1]);
							
							PriceAlert priceAlert = new PriceAlert(Long.parseLong(alert.get(0)), alert.get(1), BigDecimal.valueOf(Long.parseLong(alert.get(2).replaceAll(" ", ""))));
							alertService.processPriceAlert(priceAlert);
						}
					}else if(message instanceof ObjectMessage)						
						if(message.getJMSType().equals(PRICE_ALERT_TYPE))							
							alertService.processPriceAlert( (PriceAlert) ((ObjectMessage)message).getObject() );
				} catch (JMSException e) {
					log.error("Error: ", e);
				}				
			});
			
			consumerVolumeAlert.setMessageListener( message -> {
				try{
					if(message instanceof TextMessage){
						List<String> alert = new ArrayList<>();
						if(message.getJMSType().equals(VOLUME_ALERT_TYPE)){
							String[] volumeAlertMsg = ((TextMessage)message).getText().split("\n");
							
							alert.add(volumeAlertMsg[0].split("=")[1]);
							alert.add(volumeAlertMsg[1].split("=")[1]);
							alert.add(volumeAlertMsg[2].split("=")[1]);
							
							VolumeAlert volumeAlert = new VolumeAlert(Long.parseLong(alert.get(0)), alert.get(1), Long.parseLong(alert.get(2).replaceAll(" ", "")));
							alertService.processVolumeAlert(volumeAlert);
						}
					}else if(message instanceof ObjectMessage){							
						if(message.getJMSType().equals(VOLUME_ALERT_TYPE)){
							alertService.processVolumeAlert( (VolumeAlert) ((ObjectMessage)message).getObject() );
						}
					}
				}catch(JMSException e){
					log.error("Error: ", e);
				}
			});
			
		} catch (JMSException e) {
			log.error("Error: ", e);
		}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		try {
			if(session !=null)
				session.close();
			if(connection !=null)
				connection.close();
			if(consumerPriceAlert !=null)
				consumerPriceAlert.close();
			if(consumerVolumeAlert !=null)
				consumerVolumeAlert.close();
		} catch (JMSException e) {
			log.error("Error message ", e);
		}
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.   
}
