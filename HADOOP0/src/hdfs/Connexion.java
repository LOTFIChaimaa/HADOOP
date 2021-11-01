package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/** Cette classe permet de gerer la communication entre HdfsClient 
 * et HdfsServer.
 */


public class Connexion {
	
	 /** La socket de connexion */
    	Socket socket;
    	/** stream entrant */
    	ObjectOutputStream oos;
    	/** stream sortant */
   	 ObjectInputStream ois;
    
    	/** Constructeur : On initialise le socket et les Object stream 
     	 * à partir du socket de communication qui contient les stream
     	 * @param socket 
     	* */
	public Connexion(Socket socket) {
		this.socket= socket;
        	try {
            	oos = new ObjectOutputStream(socket.getOutputStream());
                ois = new ObjectInputStream(socket.getInputStream());
        	} catch (IOException e) {
            		e.printStackTrace();
        	}
	}
	
	
	/** Envoyer un objet
  	  * 
          * @param object */
    	public void envoyer(Object objet) {
        	try {
        	    oos.writeObject(objet);
        	} catch (UnknownHostException e) {
        	    e.printStackTrace();
        	} catch (IOException e) {
        	    e.printStackTrace();
        	}
    	}	
	
	/** Récuperer un objet
     	* @return Object
     	*/
    	public Object recevoir() {
        	Object obj = null;
        	try {
        	    obj = (Object) ois.readObject();
        	} catch (UnknownHostException e) {
        	    e.printStackTrace();
        	} catch (IOException e) {
        	    e.printStackTrace();
        	} catch (ClassNotFoundException e) {
        	    System.out.println("Objet inconnu à la réception");
        	    e.printStackTrace();
        	}
        	return obj;
    }

	
 	/** Fermer de la socket
	     * */
	    public void Close() {
		try {
		    socket.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	
}
