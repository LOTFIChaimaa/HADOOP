package config;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.Serializable;

/**
 * Cette partie contient la configuration de notre projet 
 * Elle mettra en avant les informations commune entre les deux parties HDFS et HIDOOP
 * HDFS a un projet(fichier a fragmenter) ce projet se mettra a jour au file des traitements
 * il mettra a jour les node par RMI le name node qui possede lui aussi le projet enm question
 * Namenode dans ce projet et notre liens qui relie les deux parties HDFS et HIDOOP . 
 * 
 * 
 * */
public class Gestion implements Serializable {
	public static String PATH = "/home/mwissad/2A/Hidoop/hidoop/data";
	//les url de nos serveurs 
	public List<String> severUrl = new ArrayList<String>();
	// les url de nos workers
	public List<String> urlWorkers = new ArrayList<String>();
	// les numéros de port des noeud
	public List<Integer> portNodes = new ArrayList<Integer>();
	// la liste des fichiers initialement dans le
	public List<String> inputFileNameList = new ArrayList<String>();
	//nomfichier et son nb de fragement
	public HashMap<String, Integer> nbMaps = new HashMap<String, Integer>();
	//hashmap qui associe un fichier traité par hdfs avec une autre map contenant les couples num block et l url du worker
	//qui le stock
	public HashMap<String, HashMap<Integer, String>> workersFragRepartie = new HashMap<String, HashMap<Integer, String>>();
	
	//constructeur de la classe
	public Gestion() {
		try {
			getStructure();
		} catch (PropretyInvalideException e) {
			e.printStackTrace();
		}

	}
	
	public void getStructure() throws PropretyInvalideException {
		
		Properties proprietePorts = new Properties();
		Properties proprieteConfig = new Properties();
		Properties proprieteWorkers = new Properties();
		Properties proprieteServeur = new Properties();
		int nbFile = 0;
		
		try {
			FileInputStream isPorts = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/portNoeud.url");
			proprietePorts.load(isPorts);
			isPorts.close();
			
			FileInputStream isConf = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/confStructure.conf");
			proprieteConfig.load(isConf);
			isConf.close();
			
			FileInputStream isWorker = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/listWorkerURL.listeurl");
			proprieteWorkers.load(isWorker);
			isWorker.close();
			
			
			FileInputStream isServ = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/serverHDFS.listeurl");
			proprieteServeur.load(isServ);
			isServ.close();
			
			String property = proprieteConfig.getProperty("fileName0");

			while (property != null) {
				property = proprieteConfig.getProperty("fileName0" + nbFile);nbFile++;
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			/*les noms des fichiers  recup */
			for (int i = 0; i < nbFile; i++) {
				inputFileNameList.add(proprieteConfig.getProperty("fileName" + i));
			}
			/*recup des serveurs */
			int nbServ = 0;
			String serveur = proprieteServeur.getProperty("url0");
			while (serveur != null) {
				severUrl.add(serveur);nbServ++;
				serveur = proprieteServeur.getProperty("url" + nbServ);
			}
			
			/*recup des Workers */
			int nbWorkers = 0;
			String worker = proprieteWorkers.getProperty("url0");
			while (worker != null) {
				urlWorkers.add(worker);nbWorkers++;
				worker = proprieteWorkers.getProperty("url" + nbWorkers);
			}

			

			/* recup des port */
			int nbPorts = 0;
			String port = proprietePorts.getProperty("port0");
			while (port != null) {
				portNodes.add(Integer.parseInt(port));nbPorts++;
				port = proprietePorts.getProperty("port" + nbPorts);}
			
			for (int i = 0; i < inputFileNameList.size(); i++) {
				System.out.println(inputFileNameList.get(i));
				if (inputFileNameList.get(i) == null) {
					throw new PropretyInvalideException("structure.inputFileNameList.get(" + i + ")");
				}
			}
		}
	}
}
