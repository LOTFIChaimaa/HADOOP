package formats;

import java.text.Normalizer;

public class KV implements KVItf {
	private static final long serialVersionUID = 1L;

	public static final String SEPARATOR = "<->";
	
	public String k;
	public String v;
	
	public KV() {}
	
	public KV(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}
	
	public int rangeChar() {
		String alphabet = "abcdefghijklmnopqrstuvwxyz";
		/* Enlever les accents */
		String sansAccents = Normalizer.normalize(k,  Normalizer.Form.NFD).replaceAll("[\u0300-\u036F]", "");
		char c;
		if (sansAccents.toLowerCase().charAt(0)!= ' ') {
			c = sansAccents.toLowerCase().charAt(0);
		} else {
			c = sansAccents.toLowerCase().charAt(1);
		}
		char ch = Character.toLowerCase(c);
		int index = alphabet.indexOf(ch);
		return index;
	}
	
}
