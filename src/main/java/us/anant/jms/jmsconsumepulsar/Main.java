package us.anant.jms.jmsconsumepulsar;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.PulsarDestination;
import com.datastax.oss.pulsar.jms.PulsarTopic;

/**
 * Hello world!
 *
 */
public class Main 
{
	final static String token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg0MDA3OTEsImlhdCI6MTc0NTgwODc5MSwiaXNzIjoiZGF0YXN0YXgiLCJzdWIiOiJjbGllbnQ7MWY0ZTNmNWYtOWFhNC00ZGYwLThjMDktZGY2ZjA1Nzg4NjdlO2FXNWtkWE4wY21sbGN3PT07ZGVlOTMzNWU1NCIsInRva2VuaWQiOiJkZWU5MzM1ZTU0In0.AvlGS6pRjsPnzf87DGvUlhbLzBWnEIBrz7omdZfqnxKlSTY9Dn_YFxqYSD_f-djiNN3l5anUXve0zR-t5XxhfpZ9PqG3TDRp5SIkNNzcaPEv-8gBT6Y9m7AvUZ8BjIDffR5Sr55V-P_fpwkr16eNaXQG__u_GhksxNq7qDdTZ-0Wn4YIhdAH-e7O8kyNgdK9y7wKkWivvE75h068kU82nkSNKaLya8zQ5v8VsyDzIUI6Pxyi1EGJVS0PNMw17zctRes8MNGKGjPKHoEz9FgwkkqE2T83oaoqACXG1GfJfBXwSs162nSGNbXEm-PBCc1KMf_Jaji0Gjhb3pJArze5Aw";
	final static String pulsarUrl = "https://pulsar-gcp-useast1.api.streaming.datastax.com";
	final static String brokerUrl = "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651";
	
	final static String vwTopic = "industries/automobiles/vw";
	final static String nvwTopic = "industries/automobiles/nvw";
	
	
	
	
    public static void main( String[] args ) {
    	System.out.println("Calling Main");
    	if (args.length != 1) {
    		System.out.println("Usage: -gp(generate persistent) or -gn (generate non-persist) or -cp (consume persistent) or  -cn(consume non persistent)");
    		System.exit(1);
    	}
    	
    	System.out.println(args[0]);
    	
    	   	  
    	final String argument = args[0];
    	
    	switch(argument) {
    		case "-gp":
    			generatePersistentMessage();
    			break;
		case "-gn":
				generateNonPersistentMessage();
    			break;
    		case "-cp":
    			consumePersistentMessage();
    			break;
    		case "-cn":
    			consumeNonPersistentMessage();
    			break;
    		default:
    			break;
    	}
    	  
    }
    
	private static Map<String,Object> createPersistConfiguration() {
		final Map<String, Object> configuration = new HashMap<>();
		configuration.put("webServiceUrl", pulsarUrl);
  	  	configuration.put("brokerServiceUrl", brokerUrl);
  	  	configuration.put("authPlugin","org.apache.pulsar.client.impl.auth.AuthenticationToken");
  	  	configuration.put("authParams",token);
  	  	
  	  	return configuration;
	}
	
	private static Map<String,Object> createNonPersistConfiguration() {
		final Map<String, Object> configuration = new HashMap<>();
		configuration.put("webServiceUrl", pulsarUrl);
  	  	configuration.put("brokerServiceUrl", brokerUrl);
  	  	configuration.put("authPlugin","org.apache.pulsar.client.impl.auth.AuthenticationToken");
  	  	configuration.put("authParams",token);
  	  	
  	  	return configuration;
	}
	
	private static void generatePersistentMessage() {
		
		final Map<String, Object> configuration = createPersistConfiguration();
		
		try (final PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration)) {
			try (final JMSContext context = factory.createContext()) {
				final Destination destination = context.createQueue("persistent://" + vwTopic);
				context.createProducer().send(destination, "PERSIST" + UUID.randomUUID().toString());
			} catch(Exception ex) {
				System.out.println("Generate Produce Error : " + ex.getLocalizedMessage());
			}
		} catch(Exception ex) {
			System.out.println("Generate PulsarConnectionFactory Error : " + ex.getLocalizedMessage());
		}
	}
	
	private static void generateNonPersistentMessage() {
		final Map<String, Object> configuration = createNonPersistConfiguration();
		final Topic pulsarTopic = new PulsarTopic("non-persistent://" + nvwTopic);
		try (final PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration)) {
			try (final JMSContext context = factory.createContext()) {
				final Destination destination = context.createQueue("non-persistent://" + nvwTopic);
				final JMSProducer jmsProducer = context.createProducer();
				jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				jmsProducer.send(destination, "NON-PERSIST" + UUID.randomUUID().toString());
			} catch(Exception ex) {
				System.out.println("Generate Produce Error : " + ex.getLocalizedMessage());
			}
		} catch(Exception ex) {
			System.out.println("Generate PulsarConnectionFactory Error : " + ex.getLocalizedMessage());
		}
	}
	
	private static void consumePersistentMessage() {
		
		 final Map<String, Object> configuration = createPersistConfiguration();
		
		  try (PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration)) {
	    	  try (JMSContext context = factory.createContext()) {
	    	      Destination destination = context.createQueue("persistent://" + vwTopic);
	    	      try (JMSConsumer consumer = context.createConsumer(destination)) {
	    	          String message = consumer.receiveBody(String.class);
	    	          System.out.println(message);
	    	      } catch (Exception ex) {
	    	    	  System.out.println("Consumer Error : " + ex.getLocalizedMessage());
	    	      }
	    	  } catch (Exception ex) {
	    		  System.out.println(ex.getLocalizedMessage());
	    	  }
	   	  } catch (Exception ex) {
	   		  System.out.println(ex.getLocalizedMessage());
	   	  }
	}
	
	private static void consumeNonPersistentMessage() {
		final Map<String, Object> configuration = createNonPersistConfiguration();
		final Topic pulsarTopic = new PulsarTopic("non-persistent://" + nvwTopic);
		  try (PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration)) {
	    	  try (JMSContext context = factory.createContext()) {
	    	      try (final JMSConsumer consumer = context.createSharedConsumer(pulsarTopic, "np-jms-queue")) {
	    	          String message = consumer.receiveBody(String.class);
	    	          System.out.println(message);
	    	      } catch (Exception ex) {
	    	    	  ex.printStackTrace();
	    	    	  System.out.println("Consumer Error : " + ex.getLocalizedMessage());
	    	      }
	    	  } catch (Exception ex) {
	    		  System.out.println(ex.getLocalizedMessage());
	    	  }
	   	  } catch (Exception ex) {
	   		  System.out.println(ex.getLocalizedMessage());
	   	  }
	}

}
