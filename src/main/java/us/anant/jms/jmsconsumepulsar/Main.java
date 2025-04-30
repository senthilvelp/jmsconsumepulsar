package us.anant.jms.jmsconsumepulsar;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
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
    	final String token = """
    			eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg0MDA3OTEsImlhdCI6MTc0NTgwODc5MSwiaXNzIjoiZGF0YXN0YXgiLCJzdWIiOiJjbGllbnQ7MWY0ZTNmNWYtOWFhNC00ZGYwLThjMDktZGY2ZjA1Nzg4NjdlO2FXNWtkWE4wY21sbGN3PT07ZGVlOTMzNWU1NCIsInRva2VuaWQiOiJkZWU5MzM1ZTU0In0.AvlGS6pRjsPnzf87DGvUlhbLzBWnEIBrz7omdZfqnxKlSTY9Dn_YFxqYSD_f-djiNN3l5anUXve0zR-t5XxhfpZ9PqG3TDRp5SIkNNzcaPEv-8gBT6Y9m7AvUZ8BjIDffR5Sr55V-P_fpwkr16eNaXQG__u_GhksxNq7qDdTZ-0Wn4YIhdAH-e7O8kyNgdK9y7wKkWivvE75h068kU82nkSNKaLya8zQ5v8VsyDzIUI6Pxyi1EGJVS0PNMw17zctRes8MNGKGjPKHoEz9FgwkkqE2T83oaoqACXG1GfJfBXwSs162nSGNbXEm-PBCc1KMf_Jaji0Gjhb3pJArze5Aw
    			""";
    	Map<String, Object> configuration = new HashMap<>();
    	  configuration.put("webServiceUrl", "https://pulsar-gcp-useast1.api.streaming.datastax.com");
    	  configuration.put("brokerServiceUrl", "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651");
    	  
    	  configuration.put("authPlugin","org.apache.pulsar.client.impl.auth.AuthenticationToken");
    	  configuration.put("authParams",token);
    	  
    	  try (PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration)) {
	    	  try (JMSContext context = factory.createContext()) {
	    	      Destination destination = context.createQueue("persistent://industries/automobiles/*");
	    	      context.createProducer().send(destination, "text");
	    	      try (JMSConsumer consumer = context.createConsumer(destination)) {
	    	          String message = consumer.receiveBody(String.class);
	    	          System.out.println(message);
	    	      } catch (Exception ex) {
	    	    	  System.out.println(ex.getLocalizedMessage());
	    	      }
	    	  } catch (Exception ex) {
	    		  System.out.println(ex.getLocalizedMessage());
	    	  }
    	  } catch (Exception ex) {
    		  System.out.println(ex.getLocalizedMessage());
    	  } 
    }
}
