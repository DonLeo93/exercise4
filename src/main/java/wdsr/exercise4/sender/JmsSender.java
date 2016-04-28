package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.mail.Message;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		try{
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:91616");

	        Connection connection = connectionFactory.createConnection();
	        connection.start();

	        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

	        Destination destination = session.createQueue(queueName);
        
	        MessageProducer producer = session.createProducer(destination);
	        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	        ObjectMessage message = session.createObjectMessage(new Order(orderId, product, price));
	        message.setJMSType("Order");
	        message.setStringProperty("WDSR-System", "OrderProcessor");
	        producer.send(message);

	        session.close();
	        connection.close();
		}catch(JMSException e){
			log.error(e.toString());
		}
    }

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		try{
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:91616");

	        Connection connection = connectionFactory.createConnection();
	        connection.start();

	        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

	        Destination destination = session.createQueue(queueName);
        
	        MessageProducer producer = session.createProducer(destination);
	        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	        TextMessage message = session.createTextMessage(text);
	        producer.send(message);

	        session.close();
	        connection.close();
		}catch(JMSException e){
			log.error(e.toString());
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try{
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost:91616");

	        Connection connection = connectionFactory.createConnection();
	        connection.start();

	        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

	        Destination destination = session.createTopic(topicName);
        
	        MessageProducer producer = session.createProducer(destination);
	        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	        MapMessage message = session.createMapMessage();	        
	        for (Map.Entry<String, String> entry : map.entrySet())
	        {
	            message.setString(entry.getKey(), entry.getValue());
	        }
	        producer.send(message);

	        session.close();
	        connection.close();
		}catch(JMSException e){
			log.error(e.toString());
		}
	}
}
