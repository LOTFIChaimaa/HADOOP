package hdfs;

import config.Gestion;
import formats.Format;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.util.HashMap;
import java.util.Properties;


public class HdfsClient {

	/* Les attributs de HdfsClien*/
	private static Gestion structure;

	/** Nombre de serveurs hidoop disponible */
	private static int nbServer;

	/** le taille maximal d'un fragement. */
	private static int sizeMaxFrag = 2000;

	/** Le message à lire lors de HdfsRead */
	private static String msgRead;

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	/** Permet de supprimer les fragments d’un fichier stocké dans HDFS
     	  * 
          * @param hdfsFname
        */ 
	public static void HdfsDelete(String hdfsFname) {

		/* On recupere le nom de fichier donné on supprime son extension.*/
		String fileName = hdfsFname.replaceFirst("[.][^.]+$", "");
		/* On recupere la liste des fichiers qui ont ce nom à partir des NameNode*/
		HashMap<Integer, String> listNN = structure.workersFragRepartie.get(fileName);
		listNN.forEach((i, url) -> {
			try {
				Socket socket;
				/*On recupere l'indice correspond au URL*/
				int indiceNN = structure.urlWorkers.indexOf(url);
				/* On crée un socket qui permet l'échange entre le serveur et le client*/
				socket = new Socket(structure.severUrl.get(indiceNN), structure.portNodes.get(indiceNN));
				Connexion conx = new Connexion(socket);
				Communication cmd = new Communication(Communication.Commande.CMD_DELETE, fileName + "-bloc" + i + ".txt", 0);
				conx.envoyer(cmd);
				conx.Close();
				 /* suppression le contenu du fichier dans la node*/
				structure.workersFragRepartie.get(fileName).remove(i);
				System.out.println("le fichier " + fileName + "-bloc" + i + " a été supprimé sur le node " + indiceNN);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		/* On supprime le fichier*/
		structure.workersFragRepartie.remove(fileName);
	}
	
	/** Permet de fragmenter le fichier
    	 * 
    	 * @param fmt
    	 * @param localFSSourceFname
    	 * @param repFactor
    	*/
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {

		File file = new File(localFSSourceFname);
        String[] dir = localFSSourceFname.split("/");
		/* On recupere le nom de fichier donné on supprime son extension.*/
		String fileName = localFSSourceFname.replaceFirst("[.][^.]+$", "");
		/* taille de fichier*/
		long fileLength = file.length();
		Communication cmd = new Communication(Communication.Commande.CMD_WRITE, "", 0);
		int j;

		/* valeur de retour de read*/
		int resRead;
		/* buffer où on va stocker les carac lus */
		char[] buffer = new char[sizeMaxFrag];
		/* nombre des caractere envoyer */
		int nbCaracSent;
		/* nombre des caracteres perdus. */
		int nbCaracLost = 0;

		int nbFragment = (int) (fileLength / sizeMaxFrag);
		int nbCaracRestant = (int) (fileLength % sizeMaxFrag);

		/* On ajoute un fragement pour ne pas couper un mot sur deux et fausser le résultat*/
		if (nbCaracRestant != 0) {
			nbFragment++;
		}
        nbCaracRestant = 0;

		try {
			/* lire le fichier avec fileReader*/
			FileReader fr = new FileReader(file);
			/* Donne le numéro de node sur lequel on écrit un fragment. */
			int nmNode = 0;
			/* On recupere la liste contenant les noms de fragments */
			HashMap<Integer, String> listNN = new HashMap<Integer, String>();
			for ( int i = 0; i < nbFragment; i++) {
				/* On supprime le double slash de l'adresse */
				String nodeAdresse = structure.severUrl.get(nmNode);
				/* On crée un socket qui permet l'échange entre le serveur et le client*/
                System.out.println(nodeAdresse+"\n"+ structure.portNodes.get(nmNode) + "\n");
				Socket socket = new Socket(nodeAdresse, structure.portNodes.get(nmNode));
				Connexion conx = new Connexion(socket);
				System.out.println("Ecriture du fragment " + i + " Dans le node " + nmNode);
				/* On reverse le debut de buffer. */
				for (int k = 0; k < nbCaracLost; k++) {
					buffer[k] = buffer[sizeMaxFrag - nbCaracLost + k];
				}
				resRead = fr.read(buffer, nbCaracLost, sizeMaxFrag-nbCaracLost);
                if (i != nbFragment-1) {
				    j = sizeMaxFrag;
				    nbCaracLost = 0;
				    while (j >= 1 && buffer[j - 1] != ' ' ) {
						    j--;
						    nbCaracLost++;
				    }
				    nbCaracRestant += nbCaracLost;
				    if (nbCaracRestant >= sizeMaxFrag) {
					    nbCaracRestant -= sizeMaxFrag;
					    nbFragment++;
				    }
				    nbCaracSent = sizeMaxFrag - nbCaracLost;
				    System.out.println("Le nombre de caracteres lus :  " + nbCaracSent );
				    System.out.println( "Le nombre de caracteres envoye : " + nbCaracSent +
					                "Le nombre de caracteres perdu : " + nbCaracLost);
                } else {
                    nbCaracSent = resRead;
                    System.out.println("Le nombre de caracteres lus :  " + resRead );
				    System.out.println( "Le nombre de caracteres envoye : " + resRead);
                }
				cmd = new Communication(Communication.Commande.CMD_WRITE, "/home/"+dir[2]+"/nosave/"+dir[dir.length -1] + "-bloc" + i + ".txt", nbCaracSent);
				conx.envoyer(cmd);
				System.out.println("Envoi du fragment...");
				conx.envoyer(buffer);

				/* On ajoute le fragement au serveur corrspand*/
				listNN.put(i, structure.urlWorkers.get(nmNode));
				nmNode++;
				nmNode = nmNode%nbServer;

				conx.Close();
			}
			structure.nbMaps.put(localFSSourceFname, nbFragment);
			System.out.println("Fin de l'ecriture " +  nbFragment + " fragment ecrits.");
			structure.workersFragRepartie.put(localFSSourceFname, listNN);
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Permet de lire un fichier de nom hdfsFname à partir de HDFS. Les fragments 
         * du fichier sont lus à partir des différentes machines, concaténés et 
         * stockés localement dans un fichier de nom localFSDestFname.
        */

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		/* On recuperer le nom de fichier sans son extension, 
   		 * ça sert lors de l'envoie d'un msg à HdfsServer */
		String fileName = hdfsFname.replaceFirst("[.][^.]+$", "");
		/* On recupere la liste des fichiers qui ont ce nom à partir des NameNode*/
		HashMap<Integer, String> listNN = structure.workersFragRepartie.get(hdfsFname);

        String[] dir = hdfsFname.split("/");

		/* Fichier sur le quel on met le résultat*/
		localFSDestFname = "/home/"+dir[2]+"/nosave/"+dir[dir.length -1] + "-resfinal.txt";
		File file = new File(localFSDestFname);
		FileWriter fw;
		try {
			msgRead = new String();
			fw = new FileWriter(file);
			listNN.forEach((i, url) -> {
				try {
					/*On recupere l'indice correspond au URL*/
					int nbNode = structure.urlWorkers.indexOf(url);
					/* On supprime le double slash de l'adresse */
					String nodeAdresse = structure.severUrl.get(nbNode);
					/* On crée la socket qui permet l'échange entre ser et client*/
					Socket socket = new Socket(nodeAdresse, structure.portNodes.get(nbNode));
					Connexion conx = new Connexion(socket);
					Communication cmd = new Communication(Communication.Commande.CMD_READ, "/home/"+dir[2]+"/nosave/"+dir[dir.length -1] + "-res" + i + ".txt", 0);
					/* Envoie de l'objet*/
					conx.envoyer(cmd);
					/* on concatene les textes de tous les fragments*/
					msgRead = msgRead + (String) conx.recevoir();
					System.out.println("Lecture du fragment " + "/home/"+dir[2]+"/nosave/"+dir[dir.length -1] + "-res" + i + " sur le node " + nbNode);
					conx.Close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			fw.write(msgRead);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*Permet de récupérer url de name nome*/
	public static String getNamNodeURL() {
		String resultat = null;
		try {
			FileInputStream in = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/namenode.url");
			Properties prop = new Properties();
			prop.load(in);
			in.close();
			resultat = prop.getProperty("url");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return resultat;

	}

	public static void main(String[] args) {

		try {
			NameNodeInterface nameNodeItf = (NameNodeInterface) Naming.lookup(getNamNodeURL());
			if (nameNodeItf.structureFound()) {
				System.out.println("Namenode existant : Recuperation.");
				structure = nameNodeItf.retakeStructure();
			} else {
				System.out.println("Pas de namenode trouvé, création d'un nouveau.");
				structure = new Gestion();
			}

			nbServer = structure.severUrl.size();
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[2], null);
				break;
			case "delete":
				HdfsDelete(args[2]);
				break;
			case "write":

				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}

			/* On update le nameNode m-à-j*/
			System.out.println(structure.nbMaps.get(args[1]));
			nameNodeItf.updateStructure(structure);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
