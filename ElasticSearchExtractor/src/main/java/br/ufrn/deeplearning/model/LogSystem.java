/**
 * 
 */
package br.ufrn.deeplearning.model;

/**
 * @author jadson
 *
 */
public enum LogSystem {
	
	SIGEVENTOS(16l, "SigEventos");

	private long id;
	private String desciption;

	private LogSystem(long id, String description) {
		this.id = id;
		this.desciption = description;
	}

	public long getId() {
		return id;
	}

	public String getDesciption() {
		return desciption;
	}
}
