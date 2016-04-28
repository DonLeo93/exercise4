package wdsr.exercise4.receiver;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.math.BigDecimal;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;
import wdsr.exercise4.sender.JmsSender;
import wdsr.exercise4.sender.UniversalJmsListener;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private Session session = null;
	private MessageConsumer consumer = null;
	private Connection connection = null;
	
	static final String PRICE_ALERT_TYPE = "PriceAlert";
	static final String VOLUME_ALERT_TYPE = "VolumeAlert";
	
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

            connection = connectionFactory.createConnection();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue(queueName);

            consumer = session.createConsumer(destination);
            
                  
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
			try {
				connection.start();
				
				consumer.setMessageListener(new MessageListener() {
					
					@Override
					public void onMessage(Message message) {
						try {
							if(message instanceof ObjectMessage){
								if(message.getJMSType().equals(VOLUME_ALERT_TYPE)){
									VolumeAlert volumeAlert = (VolumeAlert)(((ObjectMessage)message).getObject());
									
									alertService.processVolumeAlert(volumeAlert);
								}else if(message.getJMSType().equals(PRICE_ALERT_TYPE)){
									Object object = ((ObjectMessage)message).getObject();
									PriceAlert priceAlert = (PriceAlert)object;
									
									alertService.processPriceAlert(priceAlert);
								}
							}else if(message instanceof TextMessage){
								if(message.getJMSType().equals(VOLUME_ALERT_TYPE)){
									String[] parts = ((TextMessage)message).getText().split("\n");
									Long timestamp = Long.parseLong(parts[0].split("=")[1]);
									String stock = parts[1].split("=")[1];
									Long volume = Long.parseLong(parts[2].split("=")[1]);
									
									alertService.processVolumeAlert(new VolumeAlert(timestamp, stock, volume));
								}else if(message.getJMSType().equals(PRICE_ALERT_TYPE)){
									String[] parts = ((TextMessage)message).getText().split("\n");
									Long timestamp = Long.parseLong(parts[0].split("=")[1]);
									String stock = parts[1].split("=")[1];
									BigDecimal price = BigDecimal.valueOf(Long.parseLong(parts[2].split("= ")[1]));
									
									alertService.processPriceAlert(new PriceAlert(timestamp, stock, price));
								}
							}
							
						} catch (JMSException e) {
							e.printStackTrace();
						}
						
					}
				});
				
//				Message message = consumer.receive(1000);
//				if(message instanceof ObjectMessage){
//					alertService.processVolumeAlert(new VolumeAlert(1,"A",1));
//				}
			} catch (JMSException e) {
				e.printStackTrace();
			}
			
			
//			if(message instanceof ObjectMessage){
//				ObjectMessage objMessage = (ObjectMessage)message;
//				if(objMessage.getObject() instanceof VolumeAlert){
//					VolumeAlert volumeAlert = (VolumeAlert)objMessage.getObject();
//					alertService.processVolumeAlert(volumeAlert);
//				}
//			}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		try {
			consumer.close();
			session.close();
	        connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
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
