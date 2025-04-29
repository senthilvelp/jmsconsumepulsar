package us.anant.jms.jmsconsumepulsar;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args ) {
    	System.out.println("Calling Main");
    	
    	Map<String, Object> configuration = new HashMap<>();
    	  configuration.put("webServiceUrl", "https://pulsar-gcp-useast1.api.streaming.datastax.com");
    	  configuration.put("brokerServiceUrl", "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651");
    	  PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration);

    	  try (JMSContext context = factory.createContext()) {
    	      Destination destination = context.createQueue("persistent://industries/automobiles/vw");
    	      context.createProducer().send(destination, "text");
    	      try (JMSConsumer consumer = context.createConsumer(destination)) {
    	          String message = consumer.receiveBody(String.class);
    	          ....
    	      }
    	  }
    }
}
