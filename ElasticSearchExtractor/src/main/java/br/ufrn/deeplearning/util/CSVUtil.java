/**
 * 
 */
package br.ufrn.deeplearning.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import br.ufrn.deeplearning.model.LogOperacao;

/**
 * @author jadson
 *
 */
public class CSVUtil {
	
	/**
	 * Append Informatino to a CSV file.
	 * 
	 * @param csvName
	 */
	public static void generateLogOperacaoCSVFile(List<LogOperacao> logsOperacao, String csvFileName) {
		
		try{
			FileWriter writer = new FileWriter(csvFileName, true);
			
			for (LogOperacao logOperacao : logsOperacao) {
				writer.append(logOperacao.convertoToCSVLine());
				writer.append("\n");
			}
			
			writer.flush();
			writer.close();
			
		}catch(IOException io) {
			
		}
		
	}
	
}
