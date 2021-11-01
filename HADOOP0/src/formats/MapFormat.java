package formats;

import java.util.ArrayList;

public class MapFormat implements Format {
	private static final long serialVersionUID = 1L;

	private ArrayList<Format> liste;
	


		
	public MapFormat(ArrayList<Format> liste) {
		this.liste = liste;
	}
	
	public void open(OpenMode mode) {
		for (int i=0;i<liste.size();i++) {
			liste.get(i).open(mode);
		}
	}
	
	public void close() {
		for (int i=0;i<liste.size();i++) {
			liste.get(i).close();
		}
	}
	
	public KV read() {
		return null;
	}
	
	public int mapFile(KV record) {
		return record.rangeChar();		
	}
	
	public void write(KV record) {
		int len = liste.size();
		int index = mapFile(record);  //choix du fichier surlequel écrire
		liste.get(index%len).write(record);
	}
	
	public long getIndex() {
		return 0;
	}
	
	
	public String getFname() {return "";}
	
	public void setFname(String fname) {}


}
