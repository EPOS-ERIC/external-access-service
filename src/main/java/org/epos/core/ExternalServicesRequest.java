package org.epos.core;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.epos.core.ssl.CustomSSLSocketFactory;
import org.epos.core.ssl.LenientX509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;



public class ExternalServicesRequest {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExternalServicesRequest.class);

	private static final ExternalServicesRequest INSTANCE = new ExternalServicesRequest();
	private static final OkHttpClient CLIENT = buildClient();

	private ExternalServicesRequest() {
	}

	public static ExternalServicesRequest getInstance() {
		return INSTANCE;
    }

	private static OkHttpClient buildClient() {
		X509TrustManager trustManager = defaultTrustManager();
		SSLContext sslContext = getLenientSSLContext(trustManager);
		OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
				.readTimeout(30, TimeUnit.SECONDS)
				.connectTimeout(10, TimeUnit.SECONDS)
				.callTimeout(30, TimeUnit.SECONDS)
				.hostnameVerifier(new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});

		try {
			clientBuilder.sslSocketFactory(new CustomSSLSocketFactory(), trustManager);
		} catch (IOException e) {
			LOGGER.error(e.getLocalizedMessage());
			clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
		}

		return clientBuilder.build();
	}

	public String requestPayload(String url) throws IOException {
		LOGGER.info("Requesting payload for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
            assert response.body() != null;
            return response.body().string();
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting payload for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
                assert response.body() != null;
                return response.body().string();
			} 
		}
	}
	
	public String requestPayloadUsingHttpsURLConnection(String url) throws IOException {
	    LOGGER.info("Requesting payload for URL -> " + url);
	    URL requestUrl = new URL(url);
	    HttpsURLConnection connection = (HttpsURLConnection) requestUrl.openConnection();

	    connection.setHostnameVerifier((hostname, session) -> true);

	    try (InputStream inputStream = connection.getInputStream();
	         BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
	        StringBuilder response = new StringBuilder();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            response.append(line);
	        }
	        return response.toString();
	    }
	}
	
	public String requestPayloadImage(String url) throws IOException {
		LOGGER.info("Requesting payload image for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
			//return Base64.encode(response.body().bytes());
            assert response.body() != null;
            return Base64.getEncoder().encodeToString(response.body().bytes());
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting payload image for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
				//return Base64.encode(response.body().bytes());
                assert response.body() != null;
                return Base64.getEncoder().encodeToString(response.body().bytes());
			} 
		}
	}
	
	public Map<String, List<String>> requestHeaders(String url) throws IOException {
		LOGGER.info("Requesting headers for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
			return response.headers().toMultimap();
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting headers for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
				return response.headers().toMultimap();
			} 
		}
	}
	
	public Map<String, List<String>> requestHeadersUsingHttpsURLConnection(String url) throws IOException {
	    LOGGER.info("Requesting headers for URL -> " + url);
	    URL requestUrl = new URL(url);
	    HttpsURLConnection connection = (HttpsURLConnection) requestUrl.openConnection();

	    try {
            return connection.getHeaderFields();
	    } finally {
	        connection.disconnect();
	    }
	}
	
	
	public String getHttpStatusCode(String url) throws IOException {
		LOGGER.info("Requesting status code for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
			return Integer.toString(response.code());
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting status codes for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
				return Integer.toString(response.code());
			} 
		}
	}
	
	public String getContentType(String url) throws IOException {
		LOGGER.info("Requesting content type for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
                .head()
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
			LOGGER.info("Response: "+response.toString());
            assert response.body() != null;
            return Objects.requireNonNull(response.body().contentType()).toString();
			
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting content types for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
                assert response.body() != null;
                return Objects.requireNonNull(response.body().contentType()).toString();
			} 
		}
	}
	
	public Map<String, Object> getRedirect(String url) throws IOException {
		LOGGER.info("Requesting redirect for URL -> "+url);
		Request request = new Request.Builder()
				.url(url)
				.build();

		try (Response response = CLIENT.newCall(request).execute()) {
			LOGGER.info("Response: "+response.toString());

			Map<String, Object> responseMap = new HashMap<>();
            assert response.body() != null;
            String contentType = Objects.requireNonNull(response.body().contentType()).toString();
			String httpStatusCode = Integer.toString(response.code());
			
			responseMap.put("content-type", contentType);
			responseMap.put("httpStatusCode", httpStatusCode);
			responseMap.put("redirect-url", url);
			return responseMap;
		} catch(javax.net.ssl.SSLPeerUnverifiedException e) {
			LOGGER.error("Error on requesting redirect for URL: "+url+" cause: "+e.getLocalizedMessage());
			request = new Request.Builder()
					.url(url.replace("https://", "https://www."))
					.build();
			try (Response response = CLIENT.newCall(request).execute()) {
				Map<String, Object> responseMap = new HashMap<>();
                assert response.body() != null;
                String contentType = Objects.requireNonNull(response.body().contentType()).toString();
				String httpStatusCode = Integer.toString(response.code());
				
				responseMap.put("content-type", contentType);
				responseMap.put("httpStatusCode", httpStatusCode);
				responseMap.put("redirect-url", url);
				return responseMap;
			} 
		}
	}
	
	/*
	 * SSL Context
	 */
	
	private static SSLContext getLenientSSLContext(X509TrustManager trustManager)
	{
		X509TrustManager[] trustManagers = LenientX509TrustManager.wrap(trustManager);
		SSLContext sslContext = null;
		try {
			sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, trustManagers, new java.security.SecureRandom());
			//sslContext.getDefaultSSLParameters().setServerNames(new ArrayList<SNIServerName>());
			
		} catch (NoSuchAlgorithmException | KeyManagementException e) {
			throw new IllegalStateException(String.format(
					"Failed to obtain an SSL Context for TCS Service request. %s",
					e.toString()));
		}
		return sslContext;
	}
	
	private static X509TrustManager defaultTrustManager() 
	{
	    TrustManager[] trustManagers = defaultTrustManagerFactory().getTrustManagers();
	    if (trustManagers.length != 1) {
	        throw new IllegalStateException("Unexpected default trust managers:"
	                                        + Arrays.toString(trustManagers));
	    }
	    TrustManager trustManager = trustManagers[0];
	    if (trustManager instanceof X509TrustManager) {
	        return (X509TrustManager) trustManager;
	    }
	    throw new IllegalStateException("'" + trustManager + "' is not a X509TrustManager");
	}


	private static TrustManagerFactory defaultTrustManagerFactory() 
	{
	    try {
	        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
	        tmf.init((KeyStore) null);
	        return tmf;
	    } catch (NoSuchAlgorithmException | KeyStoreException e) {
	        throw new IllegalStateException("Can't load default trust manager", e);
	    }
	}
}
