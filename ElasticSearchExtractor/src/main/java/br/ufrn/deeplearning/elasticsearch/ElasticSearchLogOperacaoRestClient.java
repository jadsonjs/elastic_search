/**
 * 
 */
package br.ufrn.deeplearning.elasticsearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import br.ufrn.deeplearning.model.LogOperacao;
import br.ufrn.deeplearning.model.LogSystem;
import br.ufrn.deeplearning.util.CSVUtil;


/**
 * Extract information via REST API from elastic search
 * 
 * @author jadson
 *
 */
public class ElasticSearchLogOperacaoRestClient {

	/**
	 * Elastic URL
	 */
	public final static String ELASTIC_SEARCH_URL = "http://elastic.com.br"+":"+9200;

	
	/**
	 * Curl is a computer software project providing a library and command-line tool for transferring data using various protocols
	 * @param query
	 * @return
	 */
	public String getCurlCommand(String query, String nomeIndice, String nomeTipoDocumento) {
		return "\ncurl"+" -XGET "+"'"+ELASTIC_SEARCH_URL + "/" + nomeIndice + "/" + nomeTipoDocumento + "/_search?pretty'" +" -d ' \n" + query + " '\n";
	}

	/**
	 * To count in a specific index and type, the following convention is used:
	 * POST localhost:9200/test/users/_count
	 */
	public final static String TEMPLATE_URL_COUNT_ELASTICSEARCH = "%s/%s/%s/" + "_count?pretty";
	
	/**
	 * To search in a specific index and type, the following convention is used:
	 * POST localhost:9200/test/users/_search
	 */
	public final static String TEMPLATE_URL_QUERY_ELASTICSEARCH = "%s/%s/%s/" + "_search?pretty";
	
	
	/**
	 * Execute the program
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
			
		getLogOperacaoSplittedByUser();

		
	}
	
	
	
	/**
	 * Generate a set of logOperacao of specific user
	 * @throws IOException
	 */
	public static void getLogOperacaoSplittedByUser() throws IOException {

		ZoneId defaultZoneId = ZoneId.systemDefault();
		
		LocalDateTime start = LocalDateTime.now();
		start = start.minusDays(15);
		Date startDate = Date.from(start.atZone(defaultZoneId).toInstant());
		
		LocalDateTime end = LocalDateTime.now();
		end = end.minusMonths(0);
		Date endDate = Date.from(end.atZone(defaultZoneId).toInstant());
		
		long qtdresults = countLogOperacao(LogSystem.SIGEVENTOS.getId(), startDate.getTime(), endDate.getTime(), 0, 0); // 11 million
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>> "+qtdresults);
		
		int pageSize = 5000;
		
		int qtdPages = (int) (qtdresults/pageSize);
	
		
		List<LogOperacao> logsOperacao = new ArrayList<>();
		
		Integer traningFileNumber = 1;
		
		queryFor:
		for (int page = 0 ; page <= qtdPages; page++) {
			
			int size = (page+1)*pageSize;
			System.out.println(" from: "+( (page*pageSize)+1 )+" to: "+((int) ( size < qtdresults ? size : qtdresults)));
			try {
			
				logsOperacao.addAll(  
						getLogOperacao(LogSystem.SIGEVENTOS.getId(), startDate.getTime(), endDate.getTime(), page == 0 ? 0 : (page*pageSize)+1, (int) ( size < qtdresults ? size : qtdresults) ) 
						);	
				
			}catch(Exception ex) {
				System.err.println("Error, next!");
			}
			
			if(logsOperacao.size() > 50000) {
				System.out.println("splitting..... ");
				traningFileNumber = splittedByUser(traningFileNumber, logsOperacao);
				logsOperacao  = new ArrayList<>();
			}
			
			if(traningFileNumber > 1000) // we have our 1000 files, 1000 x 100 = 100.000 urls
				break queryFor;
			
		}
		
		traningFileNumber = splittedByUser(traningFileNumber, logsOperacao);
		logsOperacao  = new ArrayList<>();
		System.out.println("final split ! ");
		
	}
	
	private static Integer splittedByUser(Integer traningFileNumber, List<LogOperacao> logsOperacao) {
		
		String PATH = "/home/jadson/Documentos/deeplog/csvs/final/";
		
		Map<String, List<LogOperacao>> splittedLogsOperacao = new HashMap<>();
		
		for (LogOperacao logOperacao : logsOperacao) {
			if( splittedLogsOperacao.containsKey(logOperacao.id_registro_entrada)  ){
				List<LogOperacao> logsOfUser = splittedLogsOperacao.get(logOperacao.id_registro_entrada);
				logsOfUser.add(logOperacao);
			}else {
				List<LogOperacao> logsOfUser = new ArrayList<>();
				logsOfUser.add(logOperacao);
				splittedLogsOperacao.put(logOperacao.id_registro_entrada, logsOfUser);
			}
		}
		
		return generateTraningDataURLSplittedByUser(PATH, traningFileNumber, splittedLogsOperacao);
	}
	
	
	/**
	 * Generate the traning data files splitted by user. with just the URL information and with a fixed 100 log size.
	 * 
	 * @param PATH
	 * @param userMapLog
	 */
	private static Integer generateTraningDataURLSplittedByUser(String PATH, Integer traningFileNumber, Map<String, List<LogOperacao>> userMapLog) {
		
		List<String> userList = new ArrayList<>(userMapLog.keySet());
		
		for (int userNumber = 0 ; userNumber < userMapLog.keySet().size(); userNumber++) {
			
			String id_registro_entrada = userList.get(userNumber);
			
			List<LogOperacao> logs = userMapLog.get(id_registro_entrada);
			
			int stepSize = logs.size();
			
			
			// 100 entry by log
			List<LogOperacao> subLogs = new ArrayList<>(); 
			
			if(stepSize > 100) { // just generated if have at least 100 steps
				subLogs = logs.subList(0, 100);
				CSVUtil.generateURLTOCSVFile(subLogs, PATH+"training_"+traningFileNumber+".csv");
				System.out.println("generating training log file: "+traningFileNumber);
				traningFileNumber++;
			}
//			else {
//				subLogs = logs;
//				for(int j = logs.size(); j < 100; j++) {
//					LogOperacao log = new LogOperacao();
//					log.url = "";
//					subLogs.add( log );  // generated the empty log to complete the size 100
//			    }
//			}
				
			
		}
		
		return traningFileNumber;
	}
	
	
	
	
	
	public static void getLogOperacaoFromAllUsers() throws IOException {

		ZoneId defaultZoneId = ZoneId.systemDefault();
		
		LocalDateTime start = LocalDateTime.now();
		start = start.minusMonths(1);
		Date startDate = Date.from(start.atZone(defaultZoneId).toInstant());
		
		LocalDateTime end = LocalDateTime.now();
		end = end.minusMonths(0);
		Date endDate = Date.from(end.atZone(defaultZoneId).toInstant());
		
		long qtdresults = countLogOperacao(LogSystem.SIGEVENTOS.getId(), startDate.getTime(), endDate.getTime(), 0, 0); // 11 million
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>> "+qtdresults);
		
		int pageSize = 1000;
		
		int qtdPages = (int) (qtdresults/pageSize);
		
		String PATH = "/home/jadson/Documentos/temp_files/csvs/";
		
		String[] sets = new String[]{PATH+"test.csv", PATH+"validation.csv", PATH+"training.csv"};
		
		Random random = new Random(System.currentTimeMillis());
		
		
		for (int page = 0 ; page <= qtdPages; page++) {
			
			int size = (page+1)*pageSize;
			List<LogOperacao> logsOperacao =  getLogOperacao(LogSystem.SIGEVENTOS.getId(), startDate.getTime(), endDate.getTime(), page == 0 ? 0 : (page*pageSize)+1, (int) ( size < qtdresults ? size : qtdresults) ) ;
			
			int shuffle = random.nextInt(3);
			
			CSVUtil.generateLogOperacaoCSVFile(logsOperacao, sets[shuffle]);
		}
		
		//System.out.println(" >>>>>>>>>>>>>>>>>>>>> QTD LOGS "+logsOperacao.size());
	}
	
	
	
	
	
	
	
	
	/**
	 * Return logOperacao from a specific system. 
	 * 
	 * @param idSystem
	 * @param startDate
	 * @param endDate
	 * @throws IOException
	 */
	public static Long countLogOperacao(long idSystem, Long startDate, Long endDate, int from, int size) throws IOException{
		
		String query = getLogOperacaoQuery(idSystem, startDate, endDate, from, size);
		
		HttpClient httpClient = HttpClientBuilder.create().build();
		
		String urlRequisitadaElasticsearch = String.format(TEMPLATE_URL_COUNT_ELASTICSEARCH	, ELASTIC_SEARCH_URL, "indice_log_operacao-*", "log_operacao");
		
		System.out.println("["+Instant.now()+"] URL Query Elasticsearch: "+urlRequisitadaElasticsearch);
		
		HttpPost httpPost = new HttpPost(urlRequisitadaElasticsearch);
		
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		
		System.out.println(query);
		
		StringEntity entityParametrosQueryJson = new StringEntity(query, ContentType.APPLICATION_JSON);
		
		httpPost.setEntity(entityParametrosQueryJson);
		
		entityParametrosQueryJson.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));

		HttpResponse response = httpClient.execute(httpPost);

		if (response.getStatusLine().getStatusCode() != 200) {
			
			throw new RuntimeException("\nElasticsearch Error:"
					+ "\n HTTP de Error: " +	response.getStatusLine().getStatusCode()+" "+response.getStatusLine().getReasonPhrase()+" "
					+ "\n Details: \n" + response.getStatusLine().toString() );
			
			
		}

		return extractLogOperacaoCountResult(response.getEntity());
		
	}
	
	
	
	/**
	 * Return logOperacao from a specific system. 
	 * 
	 * @param idSystem
	 * @param startDate
	 * @param endDate
	 * @throws IOException
	 */
	public static List<LogOperacao> getLogOperacao(long idSystem, Long startDate, Long endDate, int from, int size) throws IOException{
		
		String query = getLogOperacaoQuery(idSystem, startDate, endDate, from, size);
		
		HttpClient httpClient = HttpClientBuilder.create().build();
		
		String urlRequisitadaElasticsearch = String.format(TEMPLATE_URL_QUERY_ELASTICSEARCH	, ELASTIC_SEARCH_URL, "indice_log_operacao-*", "log_operacao");
		
		System.out.println("URL Query Elasticsearch: "+urlRequisitadaElasticsearch);
		
		HttpPost httpPost = new HttpPost(urlRequisitadaElasticsearch);
		
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		
		System.out.println(query);
		
		StringEntity entityParametrosQueryJson = new StringEntity(query, ContentType.APPLICATION_JSON);
		
		httpPost.setEntity(entityParametrosQueryJson);
		
		entityParametrosQueryJson.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));

		HttpResponse response = httpClient.execute(httpPost);

		if (response.getStatusLine().getStatusCode() != 200) {
			
			throw new RuntimeException("\nElasticsearch Error:"
					+ "\n HTTP de Error: " +	response.getStatusLine().getStatusCode()+" "+response.getStatusLine().getReasonPhrase()+" "
					+ "\n Details: \n" + response.getStatusLine().toString() );
			
			
		}

		return extractLogOperacaoResult(response.getEntity());
		
	}
	
	
	
	


	/**
	 * Extract result of the elastic search query by JSON interface
	 *
	 * @param httpEntity
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	private static List<LogOperacao> extractLogOperacaoResult(HttpEntity httpEntity) throws ParseException, IOException {

		List<LogOperacao> logsOperacao = new ArrayList<>();
		
		byte[] jsonData = "".getBytes();

		String jsonResponse = EntityUtils.toString(httpEntity);
		jsonData = jsonResponse.getBytes("UTF-8");


		// System.out.println("Query Result: " + jsonResponse);

		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"));

		/* Interpret this JSON structure like this one :

		{
			  "took" : 15723,
			  "timed_out" : false,
			  "_shards" : {
			    "total" : 12,
			    "successful" : 12,
			    "failed" : 0
			  },
			  "hits" : {
			    "total" : 11510894,
			    "max_score" : 6.7750044,
			    "hits" : [ {
			      "_index" : "indice_log_operacao-2016",
			      "_type" : "log_operacao",
			      "_id" : "AVuAY6SBHg1w219YXmSv",
			      "_score" : 6.7750044,
			      "_source" : {
			        "@timestamp" : "2017-04-18T09:28:07.663Z",
			        "id_registro_entrada" : 0,
			        "url" : "https://sigeventos.ufrn.br/eventos/public/home.xhtml",
			        "data_hora_operacao" : "2016-01-08T08:53:12.0959Z",
			        "parametros" : "{}<br>",
			        "tempo" : 39,
			        "erro" : false,
			        "nome_excecao" : "",
			        "id_registro_acesso_publico" : 264305795,
			        "mensagens" : null,
			        "id_sistema" : 16,
			        "migrado_postgresql" : true
			      }
			    }

		 */

		JsonNode rootNode = mapper.readTree(jsonData);
		JsonNode hits = rootNode.path("hits").path("hits");

		if(hits.isArray() && hits.size() > 0){

			ArrayNode arrayNode = (ArrayNode) hits;
			Iterator<JsonNode> iterator = arrayNode.getElements();

			while( iterator.hasNext() ){

				JsonNode node = iterator.next();
				JsonNode _source = node.path("_source");
				
				LogOperacao log = new LogOperacao();
				log.timestamp                  = _source.get("@timestamp").toString();
				log.id_registro_entrada        = _source.get("id_registro_entrada") != null ? _source.get("id_registro_entrada").toString() : "";
				log.url                        = _source.get("url").toString();
				log.data_hora_operacao         = _source.get("data_hora_operacao").toString();
				log.parametros                 = _source.get("parametros").toString().replaceAll(";", " ");
				log.tempo                      = _source.get("tempo").toString();
				log.erro                       = _source.get("erro").toString().replaceAll(";", " ");
				log.nomeExcecao                = _source.get("nome_excecao") != null ? _source.get("nome_excecao").toString().replaceAll(";", " ") : "";
				log.message                    = _source.get("message").toString().replaceAll(";", " ");
				log.id_sistema                 = _source.get("id_sistema").toString();
				
				if( ! log.url.contains("/eventos/public/evento/") 
						&& ! log.url.contains("/eventos/public/pagina_publica/") 
						&& ! log.url.contains("/javax.faces.resource/")
						)
					logsOperacao.add(log);

			}
		}
		
		return logsOperacao;

	}

	
	/**
	 * Extract result of the elastic search query by JSON interface
	 *
	 * @param httpEntity
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	private static Long extractLogOperacaoCountResult(HttpEntity httpEntity) throws ParseException, IOException {
		
		byte[] jsonData = "".getBytes();

		String jsonResponse = EntityUtils.toString(httpEntity);
		jsonData = jsonResponse.getBytes("UTF-8");


		//System.out.println("Query Result: " + jsonResponse);

		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"));

		/* Interpret this JSON structure like this one :

		{
		    "count" : 1,
		    "_shards" : {
		        "total" : 5,
		        "successful" : 5,
		        "skipped" : 0,
		        "failed" : 0
		    }
		}

		 */

		JsonNode rootNode = mapper.readTree(jsonData);
		Long total = new Long(rootNode.get("count").toString());
		
		return total;

	}
	

	/**
	 * The elastic query to extract information
	 * 
	 * @param idSystem
	 * @param startDate
	 * @param endDate
	 * @param from
	 * @param size
	 * @return
	 */
	private static String getLogOperacaoQuery(long idSystem, Long startDate, Long endDate, int from, int size) {
		
		String query = 
				" { "+
					" \"query\":  {   "+
						
						 	" \"filtered\": {   "+
						 		
							 		" \"query\":  {   "+
							 		  		" \"bool\": { "+
							 		  				" \"must\": [ "+
							 		  					" {  \"match\":        { \"id_sistema\"  :  "+idSystem+" }  }  "+                 // select a specific system
							 		  			    " ] "+
							 		  		" } "+
									
									" }, "+
						 			
						 			
						 			/*
						 			 *  filter by the Date of the log 
						 			 */
						 			
						 			" \"filter\": { "+
								      "  \"range\": { "+
								      "    \"@timestamp\": { "+
								      "      \"gte\": "+startDate+", "+
								      "      \"lte\": "+endDate+", "+
								      "      \"format\": \"epoch_millis\" "+
								      "    }"+
								      "  }"+
								    "} "+   // filter
								      
								    
						 	
								    
						 	"} "+         // filtered
					 	
					 " }, "+   // end of general query					
					
					// pagination, by default is 10 results 
					" \"from\": "+from+", "+
					" \"size\": "+size+"  "+
					
				" } ";
		return query;
	}

	
	
	
}
