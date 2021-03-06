/**
 * 
 */
package fi.csc.fairdata.od;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.Base64;

/**
 * @author pj
 *
 */
public class Metax {

	private final String METAXREST;//"https://metax-test.csc.fi/rest/";
	private final String METAXDATASETURL; // = METAXREST+"datasets/";
	private final String METAXDIRURL;// = METAXREST+"directories/";
	private final String METAXFILEURL;// = METAXREST+"files/";
	private final static String FORMAT = "?format=json";
	public final static String DIR = "Dir";
	public final static String DATASET = "Dataset";
	String datasetid;
	String encoding = null;

	
	public Metax(String id, String auth) {
		this.datasetid = id;
		METAXREST = Application.getMetax();
		METAXDATASETURL = METAXREST+"datasets/";
		METAXDIRURL = METAXREST+"directories/";
		METAXFILEURL = METAXREST+"files/";
		try {
			encoding = Base64.getEncoder().encodeToString((auth).getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Hakee datasetin tiedot metaxista. Erityisen tärkeä on metaxrestin 3. parametri,
	 * jonka on oltava false, jottei tietoja vuoda.
	 * 
	 * @param id String dataset id
	 * @return MetaxResponse Metaxin vastauksen koodi ja sisältö
	 */
	public MetaxResponse dataset(String id) {
		return metaxrest(id, METAXDATASETURL, true, DATASET);
	}
	
	public MetaxResponse directories(String id) {
		return metaxrest(id, METAXDIRURL, true, DIR);
	}
	
	/**
	 * 
	 * @param id String dataset id
	 * @param url String basic URL to connect, some options will be added
	 * @param auth boolean true basic auth, false NO authentication: very important to not use auth
	 * because you'll get GDPR information with auth
	 * @param name String metax API to use
	 * @return MetaxResponse Object with code and content
	 */
	MetaxResponse metaxrest(String id, String url, boolean auth, String name ) {
	StringBuffer content = new StringBuffer();
	HttpURLConnection con = null;
	URL furl = null;
	try { //+/?cr_identifier="+datasetid "&recursive=true"
		String optio = name.equals(DIR) || name.equals(DATASET) ? "/files" : ""; 
		String optio2 =  name.equals(DATASET) ? "&file_fields=file_path,identifier,open_access" : ""; 
		if (name.equals(DIR))
			optio2 =  "&recursive=true";
		furl = new URL(url+id+optio+FORMAT+optio2);
		long alku =  System.currentTimeMillis();
		con = (HttpURLConnection) furl.openConnection();
		con.setRequestMethod("GET");	
		if (auth)
			con.setRequestProperty  ("Authorization", "Basic " + encoding);
		BufferedReader in = new BufferedReader(
				new InputStreamReader(con.getInputStream(), "UTF-8"));//con.getContentEncoding()
		String inputLine;
		while ((inputLine = in.readLine()) != null) {
			content.append(inputLine);
		}
		in.close();
		con.disconnect(); //??
		System.out.println(furl.toString()+ " take "+ (System.currentTimeMillis()-alku) + "ms");
		return new MetaxResponse(con.getResponseCode(), content.toString());
	} catch (IOException e2) {
		try {
		int respCode = ((HttpURLConnection)con).getResponseCode();
		InputStream es = ((HttpURLConnection)con).getErrorStream();
		int ret = 0;
		byte[] buf = new byte[8192];
		System.err.print(name +" virhetilanne "+respCode+": ");
        while ((ret = es.read(buf)) > 0) {
        	content.append(buf);
        	//System.err.write(buf); 
        	System.err.println(furl.toString());
        }
        es.close();
        return new MetaxResponse(respCode, content.toString());
        } catch (IOException e3) {
        	System.err.println(e3.getMessage());
        }
		System.err.println(e2.getMessage());
	}

	return new MetaxResponse(1234, "");
}
	
	public String file(String id) {	
		StringBuffer content = new StringBuffer();
		boolean b = false;
		HttpURLConnection con = null;
		try {
			URL url = new URL(METAXFILEURL+id+FORMAT);
			con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");			
			con.setRequestProperty  ("Authorization", "Basic " + encoding);
			BufferedReader in = new BufferedReader(
					new InputStreamReader(con.getInputStream(), "UTF-8"));//con.getContentEncoding()
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				content.append(inputLine);
				
			}
			in.close();
			con.disconnect(); //??
			return content.toString();
		} catch (IOException e) { //https://docs.oracle.com/javase/7/docs/technotes/guides/net/http-keepalive.html
	        try {
	                int respCode = ((HttpURLConnection)con).getResponseCode();
	                InputStream es = ((HttpURLConnection)con).getErrorStream();
	                int ret = 0;
	                // read the response body
	                byte[] buf = new byte[8192];
	                while ((ret = es.read(buf)) > 0) {
	                	System.err.println("File virhetilanne "+respCode+": "+buf.toString());
	                	
	                }
	                
	                // close the errorstream
	                es.close();
	        } catch (IOException e2) {
	        	System.err.println(e2.getMessage());
	        }
		}
		return null;//content.toString();
	}

}
