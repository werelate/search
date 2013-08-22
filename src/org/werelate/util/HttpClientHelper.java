package org.werelate.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.apache.commons.httpclient.methods.PostMethod;

import java.util.Map;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.InputStream;
import java.net.URLEncoder;

import nu.xom.ParsingException;
import nu.xom.Builder;

/**
 * Created by Dallan Quass
 * Date: Apr 23, 2008
 */
public class HttpClientHelper
{
   public static final int STATUS_OK = 0;

   private static final Logger logger = Logger.getLogger("org.werelate.util");
   private static final int TIMEOUT_MILLIS = 60000;
   private static final int MAX_RETRIES = 3;
   private static final int DELAY_MILLIS = 2000;
   private static final int BUF_SIZE = 32 * 1024;
   private static final int MAX_BUF_SIZE = 64 * 1024 * 1024;

   private Builder builder;
   private HttpClient client;
   private boolean addXmlHeader;

   public static String getResponse(HttpMethodBase m) throws IOException
   {
      InputStream s = m.getResponseBodyAsStream();
      int bytesRead = -1;
      int totalBytes = 0;
      int bytesToRead = BUF_SIZE;
      byte[] buf = new byte[BUF_SIZE];
      while (true) {
         bytesRead = s.read(buf, totalBytes, bytesToRead);
         if (bytesRead < 0) {
            break;
         }
         totalBytes += bytesRead;
         bytesToRead -= bytesRead;
         if (bytesToRead == 0) { // buffer full, so allocate more
            if (buf.length * 2 > MAX_BUF_SIZE) {
               throw new IOException("Response too long: "+m.getURI().toString());
            }
            byte[] temp = buf;
            buf = new byte[temp.length * 2];
            System.arraycopy(temp, 0, buf, 0, temp.length);
            bytesToRead = temp.length;
         }
      }
      if (totalBytes > 0) {
         return EncodingUtil.getString(buf, 0, totalBytes, m.getResponseCharSet());
      } else {
         return null;
      }
   }

   public HttpClientHelper(boolean addXmlHeader) {
      client = new HttpClient();
      client.getParams().setParameter("http.protocol.content-charset", "UTF-8");
      client.getParams().setParameter("http.socket.timeout", TIMEOUT_MILLIS);
      client.getParams().setParameter("http.connection.timeout", TIMEOUT_MILLIS);
      builder = new Builder();
      this.addXmlHeader = addXmlHeader;
   }

   public void executeHttpMethod(HttpMethod m) {
      // default content-type to UTF-8
      if (m instanceof PostMethod && m.getRequestHeader("Content-Type") == null) {
         m.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
      }

      // retry loop
      String msg = null;
      for (int i=0; i < MAX_RETRIES; i++)
      {
         try
         {
            client.executeMethod(m);
            if (m.getStatusCode() == 200) {
               return;
            }
            msg = "Status code="+Integer.toString(m.getStatusCode());
         }
         catch(HttpException e) {
            // do nothing
            msg = e.getMessage();
         }
         catch (IOException e) {
            // do nothing
            msg = e.getMessage();
         }
         Utils.sleep(DELAY_MILLIS);
      }
      throw new RuntimeException("Cannot communicate with server: "+msg);
   }

   public nu.xom.Document parseText(String text) throws ParsingException, IOException {
      try {
         return Utils.parseText(builder, text, addXmlHeader);
      }
      catch (nu.xom.ParsingException e) {
         logger.warning("Parsing exception: "+e.getMessage() +" while parsing: "+text);
         throw e;
      }
   }
}
