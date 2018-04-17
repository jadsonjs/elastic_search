/**
 * 
 */
package br.ufrn.deeplearning.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import br.ufrn.deeplearning.model.LogSystem;

/**
 * @author jadson
 *
 *
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-search.html
 * 
 * https://pt.slideshare.net/fhopf/java-clients-for-elasticsearch-68540081
 *
 */
public class ElasticSearchClient {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws UnknownHostException {
		
		LogSystem system = LogSystem.SIGEVENTOS;
		
		// kibana-producao.info.ufrn.br  = balancer
		TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("elastic.com.br"), 9300);
		
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress);
		
		SearchResponse searchResponse = client.prepareSearch("indice_log_operacao-*")
				//.setQuery( QueryBuilders.termQuery("multi", "test") )                          // Query
		        .setPostFilter( QueryBuilders.termQuery("id_sistema", system.getId()) )      // Filter SigEventos
		        .setFrom(0).setSize(1000).setExplain(true)
		        .get();
		
		
		SearchHit[] results = searchResponse.getHits().getHits();		
				
		client.close();
		
	}

}
