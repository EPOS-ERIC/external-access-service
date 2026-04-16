package org.epos.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.epos.api.beans.Distribution;
import org.epos.api.beans.ServiceParameter;
import org.epos.api.utility.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class ExternalAccessHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExternalAccessHandler.class);

	public static Map<String, Object> handle(Distribution distr, String kind, JsonObject conversion, Map<String, Object> requestParams) {
		
		List<ServiceParameter> distParams = distr.getParameters();
		if (distParams == null) {
			LOGGER.error("No distribution parameters provided");
		}

		if (distr.getType()==null) {
			LOGGER.error("No Type parameter provided, sending back a 503 message");
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put("httpStatusCode", "503");
			return responseMap;
		}

        Map<String, Object> responseMap = new HashMap<>();
        HashMap<String, Object> parameters = new HashMap<>();

		if (distr.getType().equals("DOWNLOADABLE_FILE")) {
			try {
                try {
                    responseMap.put("url",  distr.getDownloadURL());
                    return responseMap;
                } catch (Exception ex) {
                    LOGGER.error("Issue raised " + ex.toString() + " sending back a 503 message");
                    responseMap.put("httpStatusCode", "503");
                    return responseMap;
                }
			} catch (Exception ex) {
				LOGGER.error(ex.toString());
				return null;
			}
        }

        if (distr.getParameters() != null) {
            distr.getParameters().forEach(p -> {
                if (p.getValue() != null && !p.getValue().isEmpty())
                    parameters.put(p.getName(), p.getValue());
                if (p.getDefaultValue() != null && p.getValue() == null && p.isRequired())
                    parameters.put(p.getName(), p.getDefaultValue());
            });
        }
        if (requestParams.containsKey("format")) {
            if (ContentType.fromValue(requestParams.get("format").toString()).isPresent() && conversion == null) {
                if (distr.getParameters() != null) {
                    for (ServiceParameter p : distr.getParameters()) {
                        if (p.getProperty() != null && p.getProperty().equals("schema:encodingFormat")) {
                            parameters.remove(p.getName());
                            parameters.put(p.getName(), requestParams.get("format").toString());
                        }
                    }
                }
            }
            if (ContentType.fromValue(requestParams.get("format").toString()).isEmpty()) {
                if (distr.getParameters() != null) {
                    for (ServiceParameter p : distr.getParameters()) {
                        if (p.getProperty() != null && p.getProperty().equals("schema:encodingFormat")) {
                            parameters.remove(p.getName());
                            parameters.put(p.getName(), requestParams.get("format").toString());
                        }
                    }
                }
            }
        }

        String compiledUrl = URLGeneration.generateURLFromTemplateAndMap(distr.getEndpoint(), parameters);
        try {
            compiledUrl = URLGeneration.ogcWFSChecker(compiledUrl);
        } catch (Exception e) {
            LOGGER.error("Found the following issue whilst executing the WFS Checker, issue raised " + e.toString() + " - Continuing execution");
        }
        LOGGER.debug("URL to be executed: " + compiledUrl);

        if (kind.contains("getoriginalurl")) {
            try {
                responseMap.put("url", compiledUrl);
                return responseMap;
            } catch (Exception ex) {
                LOGGER.error("Issue raised " + ex.toString() + " sending back a 503 message");
                responseMap.put("httpStatusCode", "503");
                return responseMap;
            }
        }


		 if ((requestParams.containsKey("format") && checkFormat(requestParams.get("format").toString(), compiledUrl.contains("WFS"))) || requestParams.containsKey("pluginId")) {
			LOGGER.debug("Direct request");
            if (conversion == null) {
                LOGGER.debug("Is native GeoJSON or CovJSON");
                try {
                    String responsePayload = ExternalServicesRequest.getInstance().requestPayload(compiledUrl);
                    responseMap.put("content", responsePayload.isEmpty() ? "{}" : responsePayload);
                } catch (IOException e) {
                    LOGGER.error(e.toString());
                    LOGGER.error("Impossible to get any response from {}", compiledUrl);
                    responseMap = new HashMap<>();
                    responseMap.put("httpStatusCode", "503");
                    return responseMap;
                }
                return responseMap;
            } else {
                LOGGER.debug("Is not native GeoJSON or CovJSON");
                try {
					Map<String, String> parametersMap = new HashMap<>();
					parametersMap.put("distributionId", conversion.get("distributionId").getAsString());
					parametersMap.put("pluginId", conversion.has("plugin") ? conversion.get("plugin").getAsString() : null);
					// get the content type of the input to the converter from the parameters if
					// there is, else use the request's body content type
					if (conversion.has("requestContentType")) {
						parametersMap.put("requestContentType", conversion.get("requestContentType").getAsString());
					} else {
						ExternalServicesRequest extReq = ExternalServicesRequest.getInstance();
						try {
							String contentType = extReq.getContentType(compiledUrl);
							parametersMap.put("requestContentType", contentType);
						} catch (IOException e) {
							LOGGER.debug("Error getting service response's content type");
							parametersMap.put("requestContentType", null);
						}
					}
					parametersMap.put("responseContentType", conversion.has("responseContentType") ? conversion.get("responseContentType").getAsString() : null);					//parametersMap.put("responseContentType", conversion.get("responseContentType").getAsString());
					responseMap.put("parameters", parametersMap);
					String responsePayload = ExternalServicesRequest.getInstance().requestPayload(compiledUrl);
					responseMap.put("content", responsePayload.length()==0? "{}" : responsePayload);

					return responseMap; 
                } catch (Exception ex) {
                    LOGGER.error(
                            "Error while executing service. compiledUrl={}, kind={}, requestParams={}, conversion={}",
                            compiledUrl,
                            kind,
                            requestParams,
                            conversion,
                            ex);
					ex.printStackTrace();
                    responseMap.put("httpStatusCode", "503");
                    return responseMap;
                }
            }
        } else {
            LOGGER.debug("Redirect");
            try {
                return ExternalServicesRequest.getInstance().getRedirect(compiledUrl);
            } catch (Exception ex) {
                LOGGER.error("Issue raised " + ex.toString() + " sending back a 503 message");
                responseMap.put("httpStatusCode", "503");
                return responseMap;
            }
        }
	}

	private static boolean checkFormat(String format, boolean isWFS) {
		format = format.toLowerCase();
		return format.equals("application/geo+json")
				|| format.replaceAll("[^a-zA-Z0-9]", "").contains("geojson")
				|| format.replaceAll("[^a-zA-Z0-9]", "").contains("covjson")
				|| (format.contains("json") && isWFS)
				|| ContentType.fromValue(format).isPresent();
	}
}
