package my.awesome.project;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class CleanIndexes {

	static Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
	
	public static void main(String[] args) throws Throwable {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext("META-INF/spring/app-context.xml");
		CleanIndexes ci = context.getBean(CleanIndexes.class);
		context.registerShutdownHook();

		ci.run();
	}

	public void run() {
		try {
			delete("*", "*");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public void delete(String index, String type){
		try {
			DeleteResponse response = client.prepareDelete(index, type, "*").execute().actionGet();
		} catch(Throwable t){
			t.printStackTrace();
		}
	}

}
