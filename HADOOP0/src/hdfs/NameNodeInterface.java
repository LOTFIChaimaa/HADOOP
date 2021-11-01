package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

import config.Gestion;


public interface NameNodeInterface extends Remote {

	public int getNbFrag(String inputFname) throws RemoteException;

	public HashMap<Integer, String> getWorkersURL(String inputFname) throws RemoteException;
	
	public void updateStructure(Gestion struct) throws RemoteException;

	public boolean structureFound() throws RemoteException;

	public Gestion retakeStructure()throws RemoteException;

}
