package hdfs;

import java.io.Serializable;


/** Classe décrivant les commandes que HdfsClient envoie au HdfsServer pour écrire, lire ou supprimer
 * un fichier hdfs
 */

public class Communication implements Serializable {

	/* Enumération des commandes possibles */
	public static enum Commande {CMD_READ , CMD_WRITE , CMD_DELETE };
	
	/* Commande du client */
	private Commande cmd;
	
	/* Nom du fichier */
	private String nameFile;
	
	/* Taille du texte à écrire dans le cas où la commande est CMD_WRITE*/
	private int length;
	
	public Communication (Commande command,String name,int lgth) {
		this.cmd = command;
		this.nameFile = name;
		this.length = lgth;
	}
	
	/** Retourne la commande de la communication
	 * 
	 * @return Commande
	 */
	public Commande getCmd() {
		return this.cmd;
	}
	
	/** Retourne le nom du fichier
	 * 
	 * @return String
	 */
	public String getName() {
		return nameFile;
	}
	
	/** Retourne la taille du texte à écrire dans le cas où la commande est CMD_WRITE
	 * 
	 * @return int
	 */
	public int getLength() {
		return length;
	}
	
}
