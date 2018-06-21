/**
 * 
 */
package br.ufrn.deeplearning.logprocess;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.print.attribute.standard.PrinterLocation;

/**
 * Read the log file to generated the test database
 * 
 * @author jadson
 *
 */
public class ExtractLogInformation {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		String logFileName = "/home/jadson/Documentos/deeplog/conjunto_de_teste/log_operacao_criar_evento_2.log";
	
		try(BufferedReader br = new BufferedReader(new FileReader(logFileName))) {
		    //StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    List<LineInformation> lines = new ArrayList<>();
		    
		    while (line != null) {
		       /// sb.append(line);
		        
		        lines.add( splitLogOperacao(line) );
		        
		       // System.out.println("line => "+line);
		       // sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    //String everything = sb.toString();
		    //System.out.println(everything);
		    
		    for (LineInformation lineInformation : lines) {
				System.out.println(lineInformation);
			}
		}
		
	}
	
	/**
	 * line = "2018-06-20 18:41:21,699 598ec325-1db0-41b8-a658-116de65f0f4c e9fb8268-7340-4d83-92b2-cc1aafcf5c50 INFO  [br.ufrn.arq.seguranca.log.LogOperacao] 2018-06-20T18:41:21.0699Z http://localhost:8080/eventos/interno/menu.xhtml 33 16 172661858   false  {\"formMenuPrincipal:tabView:formCalendarioEventos:j_idt193_view\":[\"month\"],\"javax.faces.ViewState\":[\"-9042878530288200042:-2780710332792365200\"],\"formMenuPrincipal:tabView_tabindex\":[\"2\"],\"javax.faces.partial.execute\":[\"formMenuPrincipal:tabView\"],\"formMenuPrincipal:tabView_activeIndex\":[\"2\"],\"formMenuPrincipal:tabView:tabViewRelatorios_activeIndex\":[\"0\"],\"formMenuPrincipal:tabView_newTab\":[\"formMenuPrincipal:tabView:tab3\"],\"javax.faces.partial.event\":[\"tabChange\"],\"javax.faces.behavior.event\":[\"tabChange\"],\"javax.faces.partial.ajax\":[\"true\"],\"javax.faces.source\":[\"formMenuPrincipal:tabView\"],\"formMenuPrincipal\":[\"formMenuPrincipal\"],\"formMenuPrincipal:tabView:formCalendarioEventos\":[\"formMenuPrincipal:tabView:formCalendarioEventos\"]}";
	 * @param line
	 * @return
	 */
	public static LineInformation splitLogOperacao(String line) {
		
		String[] values = line.split(" ");
		
		return new LineInformation(values[7], values[8], values[9], values[10], values[11], values[16] );
		
	}
	
	
}

class LineInformation {
	

	String timeStamp;
	String url;
	Integer time;
	Integer systemId;
	Long userSessionId;
	String parameter;
	
	public LineInformation(String timeStamp, String url, String time, String systemId, String userSessionId, String parameter) {
		this.timeStamp = timeStamp;
		this.url = url;
		this.time = new Integer(time);
		this.systemId = new Integer(systemId);
		this.userSessionId = new Long(userSessionId);
		this.parameter = parameter;
	}

	@Override
	public String toString() {
		return "LineInformation [timeStamp=" + timeStamp + ", url=" + url + ", time=" + time + ", systemId=" + systemId
				+ ", userSessionId=" + userSessionId + ", parameter=" + parameter + "]";
	}
	
}


