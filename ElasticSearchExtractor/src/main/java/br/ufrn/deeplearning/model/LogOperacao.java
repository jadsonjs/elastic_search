/**
 * 
 */
package br.ufrn.deeplearning.model;

/**
 * log operacao data
 * 
 * @author jadson
 *
 */
public class LogOperacao {

	public String timestamp;
	public String id_registro_entrada;
	public String url;
	public String data_hora_operacao;
	public String parametros;
	public String tempo;
	public String erro;
	public String nomeExcecao;
	public String message;
	public String id_sistema;
	
	public String convertoToCSVLine() {
		return id_registro_entrada+";"+id_sistema+";"+data_hora_operacao+";"+url+";"+parametros+";"+tempo+";"+erro+";"+nomeExcecao+";";
	}
	
	@Override
	public String toString() {
		return "LogOperacao [timestamp=" + timestamp + ", id_registro_entrada=" + id_registro_entrada + ", url=" + url
				+ ", data_hora_operacao=" + data_hora_operacao + ", parametros=" + parametros + ", tempo=" + tempo
				+ ", erro=" + erro + ", nomeExcecao=" + nomeExcecao + ", message=" + message + ", id_sistema="
				+ id_sistema + "]";
	}
	
	
	
}
