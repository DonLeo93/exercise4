package wdsr.exercise4.sender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import wdsr.exercise4.Order;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/spring-sendtest-context.xml" })
public class SendTest {
	static final String QUEUE_NAME = "MY.QUEUE";
	static final String TOPIC_NAME = "MY.TOPIC";
	
	@Autowired
	private UniversalJmsListener listener;
	
	private JmsSender service;
	
	@Before
	public void setUp() {
		listener.reset();
		service = new JmsSender(QUEUE_NAME, TOPIC_NAME);
	}
	
    @Test
    @DirtiesContext
    public void shouldSendObjectMessageToQueue() throws InterruptedException, JMSException {
    	// given
    	final int orderId = 3;
    	final String product = "Rice";
    	final BigDecimal price = BigDecimal.TEN;
    	
    	// when
    	service.sendOrderToQueue(orderId, product, price);
    	listener.waitForMessage(3, TimeUnit.SECONDS);
    	
    	// then
    	Message receivedMessage = listener.getReceivedObjectMessage();
    	assertTrue(receivedMessage instanceof ObjectMessage);
    	Object received = ((ObjectMessage)receivedMessage).getObject();
    	assertTrue(received instanceof Order);
    	Order order = (Order)received;
    	assertEquals(orderId, order.getId());
    	assertEquals(product, order.getProduct());
    	assertEquals(price, order.getPrice());
    	assertEquals("Order", receivedMessage.getJMSType());
    	assertEquals("OrderProcessor", receivedMessage.getStringProperty("WDSR-System"));
    	assertEquals(false, listener.wasPubSub());
    }
    
    @Test
    @DirtiesContext
    public void shouldSendTextMessageToQueue() throws InterruptedException {
    	// given
    	final String text = "testText";
    	
    	// when
    	service.sendTextToQueue(text);
    	listener.waitForMessage(3, TimeUnit.SECONDS);
    	
    	// then
    	String received = listener.getReceivedText();
    	assertEquals(text, received);
    	assertEquals(false, listener.wasPubSub());
    }
    
    @Test
    @DirtiesContext
    public void shouldSendMapMessageToTopic() throws InterruptedException {
    	// given
    	Map<String, String> map = new HashMap<>();
    	map.put("key1", "val1");
    	map.put("key2", "val2");
    	
    	// when
    	service.sendMapToTopic(map);
    	listener.waitForMessage(3, TimeUnit.SECONDS);
    	
    	// then
    	@SuppressWarnings("unchecked") Map<String, String> received = listener.getReceivedMap();
    	assertEquals(map, received);
    	assertEquals(true, listener.wasPubSub());
    }   
}