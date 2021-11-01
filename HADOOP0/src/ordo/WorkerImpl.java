package ordo;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.net.*;

import formats.Format;
import formats.Format.OpenMode;
import map.MapReduce;
import map.Mapper;
import map.Reducer;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
	private static final long serialVersionUID = 1L;
	String URL ;

	
	//constructeur 
	protected WorkerImpl() throws RemoteException{
	}
	
	
	public class MultiThreadedMap implements Runnable {
		
		Mapper m; 
		Format reader;
		Format writer;
		CallBack cb;
		
		public MultiThreadedMap(Mapper map, Format r, Format w, CallBack callback) {
			m = map;
			reader = r;
			writer = w;
			cb = callback;
		}
		
		
		@Override
		public void run() {
			try{
		        System.out.println("Commence le map \n ");
				OpenMode modeRead = Format.OpenMode.R;
				OpenMode modeWrite  = Format.OpenMode.W;
				//lancement de map sur les fragments de fichier 
				reader.open(modeRead);
				writer.open(modeWrite);
		        
				m.map(reader, writer);
				//fermeture des fichiers de lecture et d'écriture 
		        reader.close();
				writer.close(); 
				System.out.println("Worker sends CallBack \n ");
				cb.give();
		       
				System.out.println("Map executed successfully");
			}catch (Exception e ){
				 e.printStackTrace();
			}
			
		}
		
	}
	
	
	public class MultiThreadedReduce implements Runnable {
			
		    Reducer red; 
			Format reader;
			Format writer;
			CallBack cb;
			
			public MultiThreadedReduce(Reducer reducer, Format r, Format w, CallBack callback) {
				red = reducer;
				reader = r;
				writer = w;
				cb = callback;
			}
			
			
			@Override
			public void run() {
				try{
			        System.out.println("Commence le reduce \n ");
					OpenMode modeRead = Format.OpenMode.R;
					OpenMode modeWrite  = Format.OpenMode.W;
					//lancement des reduce sur les fragments de fichier 
					reader.open(modeRead);
					writer.open(modeWrite);
			        
					red.reduce(reader, writer);
					//fermeture des fichiers de lecture et d'écriture 
			        reader.close();
					writer.close(); 
					System.out.println("Worker sends CallBack \n ");
					cb.give();
				       
					System.out.println("Reduce executed successfully");
				}catch (Exception e ){
					 e.printStackTrace();
				}
				
			}
			
		}
	
	
	
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException{
		try {
			Thread proc = new Thread(new MultiThreadedMap(m,reader,writer,cb));
			proc.start();
		} catch (Exception e ){
			 e.printStackTrace();
		}
	}
	
	public void runReduce (Reducer r, Format reader, Format writer, CallBack cb) throws RemoteException{
		try {
			Thread proc = new Thread(new MultiThreadedReduce(r,reader,writer,cb));
			proc.start();
		} catch (Exception e ){
			 e.printStackTrace();
		}
	}

	public static void main(String args[]) {
	//vérifier la validité de l'argument 
		try {
			if (args.length < 1) {
				System.out.println("Argument non valide , la bonne utilisation est la suivante :");
				System.out.println("java WorkerImpl port--inserer un numèro de port valide-- idint pour l id du worker");
				System.exit(1);
			}
			
			int idInt = Integer.parseInt(args[1]);
			int port  = Integer.parseInt(args[0]);
			WorkerImpl workerId = new WorkerImpl();
			workerId.URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/worker" + idInt ;
			
		   
			//creation de serveur lie au port entrer par l'utilisateur en ligne de commande  en argument 0
			try {
				LocateRegistry.getRegistry(port);
				Naming.rebind(workerId.URL, workerId);
				System.out.println("Registry déja existant");
			}
			catch(Exception e ) {
				try{
					System.out.println("Registry inexistant, creation du registry");
					LocateRegistry.createRegistry(port);
					Naming.rebind(workerId.URL, workerId);
				    } catch (Exception ex) {
					ex.printStackTrace();
					System.exit(0);
				}
			}
			System.out.println("worker" + workerId.URL + " bound in registry" + port);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}		
	
		
	

	

}
