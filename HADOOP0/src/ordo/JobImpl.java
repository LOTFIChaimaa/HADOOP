package ordo;

import map.MapReduce;

import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.MapFormat;
import hdfs.HdfsClient;
import formats.Format.OpenMode;
import hdfs.*;

public class JobImpl extends UnicastRemoteObject implements JobInterface , JobInterfaceX {
	private static final long serialVersionUID = 1L;
	
	private static int nbMaps;
	
	private static MapReduce mapReduce;
	ReentrantLock lock = new ReentrantLock(); 
	
	
	private static Format.Type inputFormat;
	private static Format.Type outputFormat;
	
	
	private static String Filename;
	private static String inputFname;
	private static String outputFname;
	
	private static SortComparator sortComparator;
	private static Format inFormat;
	private static Format outFormat;
	
	private static Worker w;
	private static HashMap<Integer, String> workersURL;
		
	//contructeur 
	public JobImpl() throws RemoteException {
	}
	
	
	//Utilisation de name node pour la récupération du fichier de config 
	public void setProperties() throws RemoteException, WrongFileNameException {
		NameNodeInterface nameNode = null;

		try {
			FileInputStream inputFileStream = new FileInputStream("/home/mwissad/2A/Hidoop/hidoop/src/config/hdfsclientinfo/namenode.url");
			Properties proprietie = new Properties();
			proprietie.load(inputFileStream);
			inputFileStream.close();
			String nameNodeURL = proprietie.getProperty("url");
             

			nameNode = (NameNodeInterface) Naming.lookup(nameNodeURL);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			workersURL = new HashMap<Integer, String>();
			nbMaps = nameNode.getNbFrag(Filename);
			workersURL = nameNode.getWorkersURL(Filename);
		}
	}
	
	// Génération du la liste de formats contenant les fichiers fragments à remplir
	public MapFormat listeFormats(int nbMaps, String[] dir, String[] res) {
		ArrayList<Format> liste = new ArrayList<Format>();
		for (int i = 0; i < nbMaps; i++) {
			liste.add(new KVFormat("/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-resMap" + i + "." + res[1]));
		}
		return new MapFormat(liste);
	}
	
		public void startJob(MapReduce mr) {
			


			mapReduce = mr;

			String URL;

			try {
				//Récupération du fichier de configuration les propriétés nécéssaire pour le lancement du job
				setProperties();
                
        
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}

			//Récuparation du nom de fichier sans l'extension
			String[] res = Filename.split("[.]");

            // Récupération du nom d'utilisateur
            String[] dir = Filename.split("/");
            
            //Génération du MapFromat 
			outFormat = listeFormats(nbMaps,dir, res);
			
			try {
				CallBack cb = new CallBackImpl();
				for (int id = 0; id < nbMaps; id++) {
					//Création des noms de fichiers blocs 
					inputFname = "/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-bloc" + id + "." + res[1];
	                
	           

					//def du format en entrée
					 if (inputFormat == Format.Type.KV) {
						 inFormat = new KVFormat(inputFname);
					}else if (inputFormat == Format.Type.LINE) {
						inFormat = new LineFormat(inputFname);
					} 

					
					try {

						URL = workersURL.get(id);
						
						//Appel du worker :
						System.out.println("Lancement du map : " + URL);
	                    
						w = (Worker) Naming.lookup(URL);
	                    
						w.runMap(mr, inFormat, outFormat, cb);
	                     
	                    

					} catch (NotBoundException nbe) {
						System.out.println("pas de worker disponible");
						System.exit(0);
					} catch (RemoteException re) {
						System.out.println("Error dans le rmi " + re.getCause());
						System.exit(0);
					} catch (Exception e) {
						System.out.println("Error" + e);
						System.exit(0);
					}
				}
				
				
				for (int i = 0; i < nbMaps; i++) {
					try {
						cb.take();
					} catch (Exception e) {
						e.printStackTrace();
					}					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
			System.out.println("All the maps are finished");
			
			try {
				this.startReduce(nbMaps, mr);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		public void startReduce(int nbReduces, MapReduce mr) throws RemoteException {
			
			mapReduce = mr;

			String[] res = Filename.split("[.]");

			String[] dir = Filename.split("/");
			
			CallBack cb = new CallBackImpl();

			for(int i=0; i<nbReduces; i++) {

				//Nom normalisé du fichier d'entrée au réduce :
				inputFname = "/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-resMap" + i + "." + res[1];
				inFormat = new KVFormat(inputFname);

				//Nom du fichier de résultat :
				outputFname = "/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-res" + i + "." + res[1];
				outFormat = new KVFormat(outputFname);


				inFormat.open(OpenMode.R);
				outFormat.open(OpenMode.W);

			    String URL = workersURL.get(i);
			    
				try {
					w = (Worker) Naming.lookup(URL);
					
					w = (Worker) Naming.lookup(URL);
					w.runReduce(mr, inFormat, outFormat, cb);
				} catch (MalformedURLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (NotBoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}
			
			for (int i = 0; i < nbReduces; i++) {
				try {
					cb.take();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			
			inFormat.close();
			outFormat.close();
			
			System.out.println("Concaténation des fichiers résulats!");
			String[] args = {"read"," ",Filename};
			HdfsClient.main(args);
			
			for(int i=0; i<nbReduces; i++) {

				File file = new File("/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-resMap" + i + "." + res[1]);
				file.delete();
				file = new File("/home/" + dir[2] + "/nosave/" + dir[dir.length -1] + "-res" + i + "." + res[1]);
				file.delete();
			}		
			
			

		}
		
		public void setInputFname(String fname) {
			Filename = fname;
		}

		public void setOutputFname(String fname) {
			outputFname = fname;
		}
		
		public void setInputFormat(Format.Type ft) {
			inputFormat = ft;
		}

		public void setOutputFormat(Format.Type ft) {
			outputFormat = ft;
		}
		

		public void setSortComparator(SortComparator sc) {
			sortComparator = sc;
		}

		public int getNumberOfMaps() {
			return nbMaps;
		}

		public Format.Type getInputFormat() {
			return inputFormat;
		}

		public Format.Type getOutputFormat() {
			return outputFormat;
		}

		public String getInputFname() {
			return Filename;
		}

		public String getOutputFname() {
			return outputFname;
		}

		public SortComparator getSortComparator() {
			return sortComparator;
		}


		

	}


	
