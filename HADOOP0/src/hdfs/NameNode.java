package hdfs;

import java.net.*;
import config.Gestion;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.HashMap;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {

	/* serialVersionUID */
	private static final long serialVersionUID = 1L;
	/*l URL*/
	private static String url;
	/*Contient des infos sur la configs des serveurs... pour m-à-j de nameNode*/
	private Gestion structure;
	/*Numero de port*/
	private static int port = 8015;

	public NameNode() throws RemoteException {
	}
	
	/** Définir les getters and setters de la structure de projets*/ 
	/*Setter*/
	public void updateStructure(Gestion struct) throws RemoteException {
		structure = struct;
		System.out.println("M-à-j de la structure");
	}

	/** Getter*/
	public Gestion retakeStructure() throws RemoteException {
		return structure;
	}

	/* Recuperer le nombre de fragements*/
	public int getNbFrag(String inputFname) throws RemoteException {
		return structure.nbMaps.get(inputFname);
	}

	/*Verifier si la structure existe bien*/
	public boolean structureFound() throws RemoteException{
		return (structure != null);
	}
	/* Recuperer les url des workers*/
	public HashMap<Integer, String> getWorkersURL(String inputFname) throws RemoteException {
		return structure.workersFragRepartie.get(inputFname);
	}
	

	public static void main(String args[]) {
		try {
			NameNode nameNode = new NameNode();
			url = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/NameNode";
            System.out.println("Création du NameNode");
			try {
				LocateRegistry.getRegistry(port);
				Naming.rebind(url, nameNode);
			} catch (Exception e) {
				try {
					LocateRegistry.createRegistry(port);
					Naming.rebind(url, nameNode);
                    
				} catch (Exception ex) {
					ex.printStackTrace();
					System.exit(0);
				}
			}

		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

	}

} 
        
