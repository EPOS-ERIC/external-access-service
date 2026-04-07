package org.epos.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.epos.api.beans.Distribution;
import org.epos.api.beans.ErrorMessage;
import org.epos.api.utility.Utils;
import org.epos.core.ExecuteItemGenerationJPA;
import org.epos.core.ExecuteItemGenerationSQL;
import org.epos.core.ExternalServicesRequest;
import org.epos.router_framework.domain.Actor;
import org.epos.router_framework.domain.BuiltInActorType;
import org.epos.router_framework.domain.Response;
import org.epos.router_framework.types.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@jakarta.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2021-10-11T14:51:06.469Z[GMT]")
@RestController
public class ExecuteOGCApiController extends ApiController implements ExecuteOGCApi {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteOGCApiController.class);

	private final ObjectMapper objectMapper;

	private final HttpServletRequest request;

	@org.springframework.beans.factory.annotation.Autowired
	public ExecuteOGCApiController(ObjectMapper objectMapper, HttpServletRequest request) {
		super(request);
		this.objectMapper = objectMapper;
		this.request = request;
	}

	@Override
	public ResponseEntity<Object> tcsconnectionsOGCExecuteGet(
		@NotNull @Parameter(in = ParameterIn.PATH, description = "the id of item to be executed" ,required=true,schema=@Schema()) @PathVariable("instance_id") String id,
		@Parameter(in = ParameterIn.QUERY, description = "pluginId", schema = @Schema()) @Valid @RequestParam(value = "pluginId", required = false) String pluginId,
		@Parameter(in = ParameterIn.QUERY, description = "input format for the plugin execution", schema = @Schema()) @Valid @RequestParam(value = "inputFormat", required = false) String inputFormat,
		@Parameter(in = ParameterIn.QUERY, description = "output format requested", schema = @Schema()) @Valid @RequestParam(value = "outputFormat", required = false) String outputFormat
	) {

		Map<String, Object> idMap = new HashMap<>();
		idMap.put("id", id);

		Map<String, Object> parametersMap = new HashMap<>();
		parametersMap.put("query", request.getQueryString());

		// Validate the parameters
		if (pluginId != null && !(outputFormat != null) && !(inputFormat != null))
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);

		return redirectRequest(ServiceType.EXTERNAL, idMap, parametersMap);
	}

	private ResponseEntity<Object> redirectRequest(ServiceType service, Map<String, Object> requestParams, Map<String, Object> otherParams) {

		Distribution response = ExecuteItemGenerationSQL.generate(requestParams);
		Response conversionResponse = null;
		JsonObject conversion = null;

		// If pluginId is specified in the request, we can assume that the distribution
		// should be converted using that plugin and those formats
		if (response.getId() != null && requestParams.containsKey("pluginId")) {
			conversion = new JsonObject();
			conversion.addProperty("distributionId", response.getId());
			conversion.addProperty("plugin", requestParams.get("pluginId").toString());
			conversion.addProperty("responseContentType", requestParams.get("format").toString());
			conversion.addProperty("requestContentType", requestParams.get("inputFormat").toString());
		}

		String compiledUrl = response.getServiceEndpoint().split("\\?")[0].replaceAll("\\?", "")+"?"+otherParams.get("query");

		HttpHeaders httpHeaders = new HttpHeaders();

		Map<String, List<String>> headers = new HashMap<String, List<String>>();
		try {
			headers = ExternalServicesRequest.getInstance().requestHeaders(compiledUrl);
		} catch (IOException e1) {
			LOGGER.error("Error on retrieving headers: "+e1.getLocalizedMessage());
			try {
				headers = ExternalServicesRequest.getInstance().requestHeadersUsingHttpsURLConnection(compiledUrl);
			} catch (IOException e2) {
				LOGGER.error("Error on retrieving headers: "+e2.getLocalizedMessage());
			}
		}

		// Copy upstream headers except any CORS-related ones to avoid duplicates with
		// @CrossOrigin
		for (String key : headers.keySet()) {
			if (key == null)
				continue;
			String lowerKey = key.toLowerCase();
			if (lowerKey.startsWith("access-control-")) {
				continue;
			}
			httpHeaders.put(key, headers.get(key));
		}

		try {

			if(compiledUrl.contains("GetFeatureInfo") && conversion!=null) {
				Map<String, Object> responseMap = new HashMap<>();
				Map<String, String> parametersMap = new HashMap<>();
				parametersMap.put("operation", conversion.get("operation").getAsString());
				parametersMap.put("requestContentType", conversion.get("requestContentType").getAsString());
				parametersMap.put("responseContentType", conversion.get("responseContentType").getAsString());
				responseMap.put("parameters", parametersMap);
				String responsePayload = "{}";
				try {
					responsePayload = ExternalServicesRequest.getInstance().requestPayload(compiledUrl);
				}catch(IOException e) {
					LOGGER.error("Error on retrieving payload: "+e.getLocalizedMessage());
					try {
						responsePayload = ExternalServicesRequest.getInstance().requestPayload(compiledUrl);
					} catch (IOException e1) {
						LOGGER.error("Error on retrieving payload: "+e1.getLocalizedMessage());
					}
				}
				responseMap.put("content", responsePayload.length()==0? "{}" : responsePayload);
				conversionResponse = doRequest(ServiceType.EXTERNAL, Actor.getInstance(BuiltInActorType.CONVERTER), responseMap);
				if(conversionResponse!=null) {
					JsonObject outputResponse = Utils.gson.fromJson(conversionResponse.getPayloadAsPlainText().get(), JsonObject.class).getAsJsonObject();

					return ResponseEntity.status(HttpStatus.OK)
							.headers(httpHeaders)
							.body(outputResponse.get("content").getAsJsonObject().toString());
				}else {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error missing conversion from "+response.getDistributionid());
					return ResponseEntity.status(HttpStatus.NO_CONTENT)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}
			}

			if(compiledUrl.contains("GetFeatureInfo") && conversion==null){
				LOGGER.debug("Redirect GetFeatureInfo");

				Map<String,Object> handlerResponse = ExternalServicesRequest.getInstance().getRedirect(compiledUrl);

				int _httpStatusCode = Integer.parseInt((String) handlerResponse.get("httpStatusCode"));
				HttpStatus httpStatusCode = HttpStatus.valueOf(_httpStatusCode);

				String redirectUrl = (String) handlerResponse.get("redirect-url");
				String contentType = (String) handlerResponse.get("content-type");
				if (StringUtils.isBlank(redirectUrl) || StringUtils.isBlank(contentType)) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error on get redirect url of an external webservice: "+response.getDistributionid()+ " causing "+httpStatusCode);
					errorMessage.setHttpCode(handlerResponse.get("httpStatusCode").toString());
					if(handlerResponse.containsKey("redirect-url")) errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
					if(handlerResponse.containsKey("content-type")) errorMessage.setContentType(handlerResponse.get("content-type").toString());
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}

				httpHeaders.add("Location", redirectUrl);
				httpHeaders.add("content-type", contentType);
				return ResponseEntity.status(HttpStatus.FOUND)
						.headers(httpHeaders)
						.body(new JsonObject().toString());
			}
			
			if(compiledUrl.contains("GetCapabilities") ||
					compiledUrl.contains("GetMap") ||
					compiledUrl.contains("GetTile")) {
				httpHeaders = new HttpHeaders();

				httpHeaders.add("Location", compiledUrl);
				httpHeaders.add("content-type", ExternalServicesRequest.getInstance().getContentType(compiledUrl));
				LOGGER.info("Http headers: "+httpHeaders.toString());
				LOGGER.info("Compiled URL: "+compiledUrl.toString());
				return ResponseEntity.status(HttpStatus.FOUND)
						.headers(httpHeaders)
						.body(new JsonObject().toString());
				
			}
/*
			if(compiledUrl.contains("GetCapabilities")) {
				LOGGER.debug("Redirect GetCapabilities");

				Map<String,Object> handlerResponse = ExternalServicesRequest.getInstance().getRedirect(compiledUrl);

				int _httpStatusCode = Integer.parseInt((String) handlerResponse.get("httpStatusCode"));
				HttpStatus httpStatusCode = HttpStatus.valueOf(_httpStatusCode);

				String redirectUrl = (String) handlerResponse.get("redirect-url");	
				String contentType = (String) handlerResponse.get("content-type");
				if (StringUtils.isBlank(redirectUrl) || StringUtils.isBlank(contentType)) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error on get redirect url of an external webservice: "+response.getDistributionid()+ " causing "+httpStatusCode);
					errorMessage.setHttpCode(handlerResponse.get("httpStatusCode").toString());
					if(handlerResponse.containsKey("redirect-url")) errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
					if(handlerResponse.containsKey("content-type")) errorMessage.setContentType(handlerResponse.get("content-type").toString());
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}

				httpHeaders.add("Location", redirectUrl);
				httpHeaders.add("content-type", contentType);
				return ResponseEntity.status(HttpStatus.FOUND)
						.headers(httpHeaders)
						.body(new JsonObject().toString());
			}

			if(compiledUrl.contains("GetMap")) {
				LOGGER.debug("Redirect GetMap");
				Map<String,Object> handlerResponse = ExternalServicesRequest.getInstance().getRedirect(compiledUrl);

				int _httpStatusCode = Integer.parseInt((String) handlerResponse.get("httpStatusCode"));
				HttpStatus httpStatusCode = HttpStatus.valueOf(_httpStatusCode);

				String redirectUrl = (String) handlerResponse.get("redirect-url");	
				String contentType = (String) handlerResponse.get("content-type");
				if (StringUtils.isBlank(redirectUrl) || StringUtils.isBlank(contentType)) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error on get redirect url of an external webservice: "+response.getDistributionid()+ " causing "+httpStatusCode);
					errorMessage.setHttpCode(handlerResponse.get("httpStatusCode").toString());
					if(handlerResponse.containsKey("redirect-url")) errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
					if(handlerResponse.containsKey("content-type")) errorMessage.setContentType(handlerResponse.get("content-type").toString());
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}

				httpHeaders.add("Location", redirectUrl);
				httpHeaders.add("content-type", contentType);
				return ResponseEntity.status(HttpStatus.FOUND)
						.headers(httpHeaders)
						.body(new JsonObject().toString());
			}

			if(compiledUrl.contains("GetTile")) {
				LOGGER.debug("Redirect GetTile");

				Map<String,Object> handlerResponse = ExternalServicesRequest.getInstance().getRedirect(compiledUrl);

				int _httpStatusCode = Integer.parseInt((String) handlerResponse.get("httpStatusCode"));
				HttpStatus httpStatusCode = HttpStatus.valueOf(_httpStatusCode);

				String redirectUrl = (String) handlerResponse.get("redirect-url");	
				String contentType = (String) handlerResponse.get("content-type");
				if (StringUtils.isBlank(redirectUrl) || StringUtils.isBlank(contentType)) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error on get redirect url of an external webservice: "+response.getDistributionid()+ " causing "+httpStatusCode);
					errorMessage.setHttpCode(handlerResponse.get("httpStatusCode").toString());
					if(handlerResponse.containsKey("redirect-url")) errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
					if(handlerResponse.containsKey("content-type")) errorMessage.setContentType(handlerResponse.get("content-type").toString());
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}

				httpHeaders.add("Location", redirectUrl);
				httpHeaders.add("content-type", contentType);
				return ResponseEntity.status(HttpStatus.FOUND)
						.headers(httpHeaders)
						.body(new JsonObject().toString());

			}
*/
			return ResponseEntity.status(HttpStatus.OK)
					.headers(httpHeaders)
					.body(ExternalServicesRequest.getInstance().requestPayload(compiledUrl));
		} catch (IOException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.headers(httpHeaders).build();
		}
	}

}
