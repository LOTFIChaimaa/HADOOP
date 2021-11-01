package ordo;
import java.rmi.Remote;
import java.rmi.RemoteException;

import map.MapReduce;


public interface CallBack extends Remote {
	public void callback (String URL, MapReduce mr) throws RemoteException;

	public void give() throws RemoteException;	
	
	public void take() throws RemoteException;
}



