package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

import map.MapReduce;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	private Semaphore semaphore;
	
	
	public CallBackImpl() throws RemoteException {
		this.semaphore = new Semaphore(0);
	}
	
	public void give() {
		this.semaphore.release();
	}
	
	public void take() {
		try {
			this.semaphore.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void callback(String URL, MapReduce mr) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

}
