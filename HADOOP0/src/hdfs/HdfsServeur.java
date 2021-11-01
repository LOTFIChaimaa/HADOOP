package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

/** Serveur Hdfs lancé sur toutes les machines **/


public class HdfsServeur extends Thread {

	/* Socket de communication */
	private static Socket s;


	public HdfsServeur(Socket s) throws IOException {
		HdfsServeur.s = s;
	}

	public void run() {
		// Communication envoyé par le Client
		Connexion conx = new Connexion(s);
		Communication cmd = (Communication) conx.recevoir();
		String fname = cmd.getName();
		// Récupération du fichier à partir de la base de données
		File file = new File(fname);

		switch (cmd.getCmd()) {
		// Le client veut lire le fichier
		case CMD_READ:
			try {				
				System.out.println("Le serveur commence la lecture du fichier");				
				// Stocker le contenu du fichier dans contenu
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				// Creation de la chaine de caractères qui sera envoyée
				String contenu = new String();
				String ligne = br.readLine();
				while (ligne != null) {
					contenu += ligne + "\n";
					ligne = br.readLine();
				}
				br.close();
				// Envoyer le contenu du fichier
				conx.envoyer(contenu);
				System.out.println("fragment du fichier envoyé");
			} catch (FileNotFoundException fnfe) {
				System.out.println("fichier lu non existant");
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;

		case CMD_WRITE:
			FileWriter fw;
			try {
				fw = new FileWriter(file);
				// Reception de la chaine de caractères correspondant au fragment
				char[] buf = new char[cmd.getLength()];
				buf = (char[]) conx.recevoir();
				// Ecrire dans le fichier
				System.out.println("Le serveur commence l'écriture dans le fichier");
				fw.write(buf, 0, cmd.getLength());
				fw.close();
				System.out.println("Le serveur a terminé l'écriture dans le fichier");
			} catch (FileNotFoundException e) {
				System.out.println("Fichier à lire introuvable");				
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;

		case CMD_DELETE:
			// Supprimer le fichier
			file.delete();		
			System.out.println("Le serveur a supprimé le fichier");

		default:
			break;
		}
	}

	public static void main(String[] args) {
		/* On recoit en argument le numÃ©ro de node du serveur */
		int port = Integer.parseInt(args[0]);

		try {
			/* Le serveur reste bloqué en attente d'une demande de client */
			ServerSocket ss;
			while (true) {
                ss = new ServerSocket(port);
				s = ss.accept();
				new HdfsServeur(s).start();
                ss.close();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
