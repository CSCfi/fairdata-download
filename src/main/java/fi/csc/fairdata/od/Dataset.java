/**
 * 
 */
package fi.csc.fairdata.od;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

/**
 * Olio pyynnön parametrien tallentamiseen ja niiden prosessoinnin kutsuminen
 * 
 * @author pj
 *
 */
public class Dataset {
	private final String id;
	private final String file; //comma separeted list
	private final String dir;
	private final HttpServletResponse response;
	private String metadata; // not in use
	
	public Dataset(String id, String file, String dir, HttpServletResponse response) {
		this.id = id;
		this.file = file;
		this.dir = dir;
		this.response = response;
	}

	/**
	 * Toimintalogiikka: 1. filteroidaan metaxin tietojen perusteella mahdolliset sallitut tiedostot
	 * 2. Jos tiedostoja on 1 kpl lähetetään se käyttäjälle
	 * 3. useamman tiedoston tapauksesa ne zipaataan
	 */
	public void käsittele() {
		Prosessor p = new Prosessor(this, file, Application.getAuth());
		List<Tiedosto> sallitut = p.metaxtarkistus(dir);
		if (null != sallitut && !sallitut.isEmpty()) {
			Tiedostonkäsittely tk = new  Tiedostonkäsittely(response);
			if (1 == p.noOfFiles())
				tk.tiedosto(sallitut.get(0));
			else
				tk.zip(sallitut, id, metadata);
		} else {
			p.virheilmoitus(404, "File(s) requested don't exists or dataset has no open files");
		}
	}
	
	
	public String getId() {
        return id;
    }
	
	public String getFile() {
        return file;
    }

	public String getDir() {
		return dir;
	}
	
	public HttpServletResponse getResponse() {
		return response;
	}

	public String getMetadata() {
		return metadata;
	}

	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}
}
