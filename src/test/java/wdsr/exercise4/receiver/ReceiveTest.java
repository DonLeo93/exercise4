package wdsr.exercise4.receiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/spring-receivetest-context.xml" })
public class ReceiveTest {
	private static final Logger log = LoggerFactory.getLogger(ReceiveTest.class);
	
	static final String QUEUE_NAME = "A.QUEUE";
	static final String PRICE_ALERT_TYPE = "PriceAlert";
	static final String VOLUME_ALERT_TYPE = "VolumeAlert";
	
	@Autowired ApplicationContext context;
	
	@Autowired
	private JmsTemplate jmsQueueTemplate;
	
	private JmsQueueReceiver service;
	
	@Before
	public void setUp() {
		purgeQueue();
		service = new JmsQueueReceiver(QUEUE_NAME);
	}
	
	@After
	public void tearDown() {
		service.shutdown();
	}	
	
	@Test
	@DirtiesContext
	public void shouldReceiveFromQueue_whenPriceAlertObjectSent() throws InterruptedException {
		// given
    	final long timestamp = System.currentTimeMillis();
    	final String stock = "GOOG";
    	final BigDecimal price = BigDecimal.TEN;
		PriceAlert priceAlert = new PriceAlert(timestamp, stock, price);
		AlertServiceTestImpl alertService = new AlertServiceTestImpl();
		
		// when
		service.registerCallback(alertService);
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			ObjectMessage objMsg = session.createObjectMessage(priceAlert);
			objMsg.setJMSType(PRICE_ALERT_TYPE);
			return objMsg;
		});
		Object received = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
				
		// then
		assertTrue(received instanceof PriceAlert);
		assertEquals(priceAlert, (PriceAlert)received);
	}
	
	@Test
	@DirtiesContext
	public void shouldReceiveFromQueue_whenVolumeAlertObjectSent() throws InterruptedException {
		// given
    	final long timestamp = System.currentTimeMillis();
    	final String stock = "GOOG";
    	final long volume = 1200;
		VolumeAlert volumeAlert = new VolumeAlert(timestamp, stock, volume);
		AlertServiceTestImpl alertService = new AlertServiceTestImpl();
		
		// when
		service.registerCallback(alertService);
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			ObjectMessage objMsg = session.createObjectMessage(volumeAlert);
			objMsg.setJMSType(VOLUME_ALERT_TYPE);
			return objMsg;
		});
		Object received = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
				
		// then
		assertTrue(received instanceof VolumeAlert);
		assertEquals(volumeAlert, (VolumeAlert)received);
	}
	
	@Test
	@DirtiesContext
	public void shouldReceiveFromQueue_whenPriceAlertTextSent() throws InterruptedException {
		// given
    	final long timestamp = System.currentTimeMillis();
    	final String stock = "GOOG";
    	final BigDecimal price = BigDecimal.TEN;
    	String alertText = String.format("Timestamp=%1d\nStock=%2s\nPrice=%3s", timestamp, stock, price);
    	log.info("AlertText=\n"+alertText);
		AlertServiceTestImpl alertService = new AlertServiceTestImpl();
		
		// when
		service.registerCallback(alertService);
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			TextMessage textMsg = session.createTextMessage(alertText);
			textMsg.setJMSType(PRICE_ALERT_TYPE);
			return textMsg;
		});
		Object received = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
				
		// then
		assertTrue(received instanceof PriceAlert);
		assertEquals(timestamp, ((PriceAlert)received).getTimestamp());
		assertEquals(stock, ((PriceAlert)received).getStock());
		assertEquals(price, ((PriceAlert)received).getCurrentPrice());
	}
	
	@Test
	@DirtiesContext
	public void shouldReceiveFromQueue_whenVolumeAlertTextSent() throws InterruptedException {
		// given
    	final long timestamp = System.currentTimeMillis();
    	final String stock = "GOOG";
    	final long volume = 500000;
    	String alertText = String.format("Timestamp=%1d\nStock=%2s\nVolume=%3d", timestamp, stock, volume);
    	log.info("alertText=\n"+alertText);
		AlertServiceTestImpl alertService = new AlertServiceTestImpl();
		
		// when
		service.registerCallback(alertService);
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			TextMessage textMsg = session.createTextMessage(alertText);
			textMsg.setJMSType(VOLUME_ALERT_TYPE);			
			return textMsg;
		});
		Object received = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
				
		// then
		assertTrue(received instanceof VolumeAlert);
		assertEquals(timestamp, ((VolumeAlert)received).getTimestamp());
		assertEquals(stock, ((VolumeAlert)received).getStock());
		assertEquals(volume, ((VolumeAlert)received).getFloatingVolume());
	}
	
	@Test
	@DirtiesContext
	public void shouldReceiveAllFromQueue_whenMoreThanOneMessageSent() throws InterruptedException {
		// given
    	final long timestamp = System.currentTimeMillis();
    	final String stock1 = "GOOG";
    	final String stock2 = "MSFT";
    	final long volume = 500000;
    	String alertText1 = String.format("Timestamp=%1d\nStock=%2s\nVolume=%3s", timestamp, stock1, volume);
    	String alertText2 = String.format("Timestamp=%1d\nStock=%2s\nVolume=%3s", timestamp, stock2, volume);
    	log.info("alertText1=\n"+alertText1);
    	log.info("alertText2=\n"+alertText2);
		AlertServiceTestImpl alertService = new AlertServiceTestImpl();
		
		// when
		service.registerCallback(alertService);
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			TextMessage textMsg = session.createTextMessage(alertText1);
			textMsg.setJMSType(VOLUME_ALERT_TYPE);			
			return textMsg;
		});
		jmsQueueTemplate.send(QUEUE_NAME, (session) -> {
			TextMessage textMsg = session.createTextMessage(alertText2);
			textMsg.setJMSType(VOLUME_ALERT_TYPE);			
			return textMsg;
		});		
		Object received1 = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
		Object received2 = alertService.tryTakeAlert(3, TimeUnit.SECONDS);
				
		// then
		assertNotNull(received1);
		assertNotNull(received2);
		assertTrue(received1 instanceof VolumeAlert);
		assertTrue(received2 instanceof VolumeAlert);
	}	


	private class AlertServiceTestImpl implements AlertService {
		private BlockingQueue<Object> alerts = new ArrayBlockingQueue<>(10);

		@Override
		public void processPriceAlert(PriceAlert priceAlert) {
			alerts.add(priceAlert);			
		}
		
		@Override
		public void processVolumeAlert(VolumeAlert volumeAlert) {
			alerts.add(volumeAlert);			
		}		
		
		public Object tryTakeAlert(long timeout, TimeUnit unit) throws InterruptedException {
			return alerts.poll(timeout, unit);
		}
	}
	
	private void purgeQueue() {	
		log.info("Setting JMS receive timeout to 100ms");
		jmsQueueTemplate.setReceiveTimeout(100);
	    while (jmsQueueTemplate.receive(QUEUE_NAME) != null) {
	        log.info("Discarded a message from "+QUEUE_NAME);
	    };
	}
}