package org.epos.api;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.epos.api.beans.Distribution;
import org.epos.api.beans.ErrorMessage;
import org.epos.api.utility.Utils;
import org.epos.core.ExecuteItemGenerationJPA;
import org.epos.core.ExecuteItemGenerationSQL;
import org.epos.core.ExternalAccessHandler;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@jakarta.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2021-10-11T14:51:06.469Z[GMT]")
@RestController
public class ExecuteApiController extends ApiController implements ExecuteApi {

	private static final String A_PROBLEM_WAS_ENCOUNTERED_DECODING = "A problem was encountered decoding: ";

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteApiController.class);

	private final ObjectMapper objectMapper;

	private final HttpServletRequest request;

	@org.springframework.beans.factory.annotation.Autowired
	public ExecuteApiController(ObjectMapper objectMapper, HttpServletRequest request) {
		super(request);
		this.objectMapper = objectMapper;
		this.request = request;
	}

	private void addDecodedParamToMap(final String paramKey, final String paramValue,
			final Map<String, Object> requestParameters) {
		addDecodedParamToMap(paramKey, paramValue, requestParameters,
				(key, decoded) -> Set.of(Map.entry(key, decoded)));
	}

	private void addDecodedParamToMap(final String paramKey, final String paramValue,
			final Map<String, Object> requestParameters,
			final BiFunction<String, String, Set<Map.Entry<String, String>>> mapDecodedParam) {
		if (!StringUtils.isBlank(paramValue)) {
			String decoded = paramValue;
			try {
				decoded = java.net.URLDecoder.decode(paramValue, StandardCharsets.UTF_8.name());
			} catch (UnsupportedEncodingException e) {
				LOGGER.warn(A_PROBLEM_WAS_ENCOUNTERED_DECODING + paramKey + ": " + paramValue, e);
			}

			final Set<Entry<String, String>> entries = mapDecodedParam.apply(paramKey, decoded);

			entries.forEach(e -> {
				requestParameters.put(e.getKey(), e.getValue());
			});
		}
	}

	public ResponseEntity<String> tcsconnectionsExecuteGet(
			@NotNull @Parameter(in = ParameterIn.PATH, description = "the id of item to be executed", required = true, schema = @Schema()) @PathVariable("instance_id") String id,
			@Parameter(in = ParameterIn.QUERY, description = "useDefaults", schema = @Schema()) @Valid @RequestParam(value = "useDefaults", required = true, defaultValue = "false") Boolean useDefaults,
			@Parameter(in = ParameterIn.QUERY, description = "output format requested", schema = @Schema()) @Valid @RequestParam(value = "format", required = false) String format,
			@Parameter(in = ParameterIn.QUERY, description = "startDate", schema = @Schema()) @Valid @RequestParam(value = "startDate", required = false) String startDate,
			@Parameter(in = ParameterIn.QUERY, description = "endDate", schema = @Schema()) @Valid @RequestParam(value = "endDate", required = false) String endDate,
			@Parameter(in = ParameterIn.QUERY, description = "bbox", schema = @Schema()) @Valid @RequestParam(value = "bbox", required = false) String bbox,
			@Parameter(in = ParameterIn.QUERY, description = "pluginId", schema = @Schema()) @Valid @RequestParam(value = "pluginId", required = false) String pluginId,
			@Parameter(in = ParameterIn.QUERY, description = "input format for the plugin execution", schema = @Schema()) @Valid @RequestParam(value = "inputFormat", required = false) String inputFormat,
			@Parameter(in = ParameterIn.QUERY, description = "params", schema = @Schema()) @Valid @RequestParam(value = "params", required = false) String params) {

		final Map<String, Object> requestParameters = decodedRequestParameters(id, useDefaults, format, startDate,
				endDate, bbox, pluginId, inputFormat, params);

		try {
			if (!StringUtils.isBlank(startDate))
				Utils.convertDateUsingPattern(startDate, Utils.EPOSINTERNALFORMAT, Utils.EPOSINTERNALFORMAT);
			if (!StringUtils.isBlank(endDate))
				Utils.convertDateUsingPattern(endDate, Utils.EPOSINTERNALFORMAT, Utils.EPOSINTERNALFORMAT);
		} catch (ParseException e1) {
			return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
		}

		// Validate the parameters
		if (requestParameters.containsKey("pluginId") &&
				!requestParameters.containsKey("format") &&
				!requestParameters.containsKey("inputFormat"))
			return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);

		return redirectRequest(requestParameters);
	}

	private Map<String, Object> decodedRequestParameters(
			final String id, final Boolean useDefaults, final String format, final String startDate,
			final String endDate, final String bbox, final String pluginId, final String inputFormat,
			final String params) {
		final Map<String, Object> requestParameters = new HashMap<>();

		addDecodedParamToMap("id", id, requestParameters);
		addDecodedParamToMap("useDefaults", Boolean.toString(useDefaults), requestParameters);
		addDecodedParamToMap("schema:startDate", startDate, requestParameters);
		addDecodedParamToMap("schema:endDate", endDate, requestParameters);
		addDecodedParamToMap("pluginId", pluginId, requestParameters);
		addDecodedParamToMap("inputFormat", inputFormat, requestParameters, (key, decoded) -> {
			String temp = decoded;
			if (temp.equals("application/epos.geo json")) {
				temp = "application/epos.geo+json";
			}
			if (temp.equals("application/epos.table.geo json")) {
				temp = "application/epos.table.geo+json";
			}
			if (temp.equals("application/epos.map.geo json")) {
				temp = "application/epos.map.geo+json";
			}
			if (temp.equals("application/geo json")) {
				temp = "application/geo+json";
			}

			return Set.of(Map.entry(key, temp));
		});
		addDecodedParamToMap("params", params, requestParameters);

		addDecodedParamToMap("format", format, requestParameters, (key, decoded) -> {
			String temp = decoded;
			if (temp.equals("application/epos.geo json")) {
				temp = "application/epos.geo+json";
			}
			if (temp.equals("application/epos.table.geo json")) {
				temp = "application/epos.table.geo+json";
			}
			if (temp.equals("application/epos.map.geo json")) {
				temp = "application/epos.map.geo+json";
			}
			if (temp.equals("application/geo json")) {
				temp = "application/geo+json";
			}

			return Set.of(Map.entry(key, temp));
		});

		addDecodedParamToMap("bbox", bbox, requestParameters, (key, decoded) -> {

			String[] bboxSplit = decoded.split(",");
			return Set.of(
					Map.entry("epos:northernmostLatitude", bboxSplit[0]),
					Map.entry("epos:easternmostLongitude", bboxSplit[1]),
					Map.entry("epos:southernmostLatitude", bboxSplit[2]),
					Map.entry("epos:westernmostLongitude", bboxSplit[3]));
		});
		return requestParameters;
	}

	private ResponseEntity<String> redirectRequest(Map<String, Object> requestParams) {

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

		Map<String, Object> handlerResponse = ExternalAccessHandler.handle(response, "execute", conversion,
				requestParams);

		LOGGER.debug("Handler response: " + handlerResponse.toString());

		String responseCode = "OK";

		if (handlerResponse.containsKey("httpStatusCode")) {
			responseCode = HttpStatus.valueOf(Integer.parseInt((String) handlerResponse.get("httpStatusCode"))).name();
		} else {
			if (handlerResponse.get("content") == null) {
				ErrorMessage errorMessage = new ErrorMessage();
				errorMessage.setMessage(
						"Error missing content from " + response.getDistributionid() + " " + handlerResponse);
				return ResponseEntity.status(HttpStatus.NO_CONTENT)
						.body(Utils.gson.toJsonTree(errorMessage).toString());
			}
			if (conversion != null)
				conversionResponse = doRequest(ServiceType.EXTERNAL, Actor.getInstance(BuiltInActorType.CONVERTER),
						handlerResponse);

			if (conversion != null && conversionResponse == null) {
				ErrorMessage errorMessage = new ErrorMessage();
				errorMessage.setMessage(
						"Error missing conversion from " + response.getDistributionid() + " " + handlerResponse);
				return ResponseEntity.status(HttpStatus.NO_CONTENT)
						.body(Utils.gson.toJsonTree(errorMessage).toString());
			}

			HttpHeaders httpHeaders = new HttpHeaders();
			if (conversion != null)
				httpHeaders.add("content-type",
						conversion.get("responseContentType").getAsString().contains("/")
								? conversion.get("responseContentType").getAsString()
								: "application/" + conversion.get("responseContentType").getAsString());
			else
				httpHeaders.add("content-type", "application/geo+json");

			JsonObject outputResponse = null;
			if (conversion != null && conversionResponse != null) {
				try {
					outputResponse = Utils.gson
							.fromJson(conversionResponse.getPayloadAsPlainText().get(), JsonObject.class)
							.getAsJsonObject();

					return ResponseEntity.status(HttpStatus.OK)
							.headers(httpHeaders)
							.body(outputResponse.get("content").getAsJsonObject().toString());
				} catch (Exception e) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error missing content from " + response.getDistributionid() + " "
							+ e.getLocalizedMessage());
					return ResponseEntity.status(HttpStatus.NO_CONTENT)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}
			} else {
				try {
					outputResponse = Utils.gson.fromJson(handlerResponse.toString(), JsonElement.class)
							.getAsJsonObject();

					return ResponseEntity.status(HttpStatus.OK)
							.headers(httpHeaders)
							.body(outputResponse.get("content").getAsJsonObject().toString());
				} catch (Exception e) {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error missing content from " + response.getDistributionid() + " "
							+ e.getLocalizedMessage());
					return ResponseEntity.status(HttpStatus.NO_CONTENT)
							.headers(httpHeaders)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}
			}
		}

		switch (responseCode) {

			default: {
				try {
					LOGGER.debug("Default case: " + responseCode);
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Received response " + responseCode + " from external webservice");
					errorMessage.setHttpCode(responseCode);
					if (handlerResponse.containsKey("redirect-url"))
						errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
					if (handlerResponse.containsKey("content-type"))
						errorMessage.setContentType(handlerResponse.get("content-type").toString());

					LOGGER.info(errorMessage.toString());

					return ResponseEntity.status(HttpStatus.valueOf(responseCode))
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			case "OK": {
				LOGGER.debug("OK case: " + responseCode);
				HttpHeaders httpHeaders = new HttpHeaders();

				if (handlerResponse.containsKey("redirect-url")
						&& handlerResponse.containsKey("content-type")
						&& handlerResponse.containsKey("httpStatusCode")) {

					int _httpStatusCode = Integer.parseInt((String) handlerResponse.get("httpStatusCode"));
					HttpStatus httpStatusCode = HttpStatus.valueOf(_httpStatusCode);

					String redirectUrl = (String) handlerResponse.get("redirect-url");
					String contentType = (String) handlerResponse.get("content-type");
					if (StringUtils.isBlank(redirectUrl) || StringUtils.isBlank(contentType)) {
						ErrorMessage errorMessage = new ErrorMessage();
						errorMessage.setMessage("Error on get redirect url of an external webservice: "
								+ response.getDistributionid() + " causing " + httpStatusCode);
						errorMessage.setHttpCode(handlerResponse.get("httpStatusCode").toString());
						if (handlerResponse.containsKey("redirect-url"))
							errorMessage.setUrl(handlerResponse.get("redirect-url").toString());
						if (handlerResponse.containsKey("content-type"))
							errorMessage.setContentType(handlerResponse.get("content-type").toString());
						return ResponseEntity.status(HttpStatus.BAD_REQUEST)
								.headers(httpHeaders)
								.body(Utils.gson.toJsonTree(errorMessage).toString());
					}

					httpHeaders.add("Location", redirectUrl);
					httpHeaders.add("content-type", contentType);
					return ResponseEntity.status(HttpStatus.FOUND)
							.headers(httpHeaders)
							.body(new JsonObject().toString());
				} else {
					ErrorMessage errorMessage = new ErrorMessage();
					errorMessage.setMessage("Error on external webservice caused by missing information: "
							+ Utils.gson.toJsonTree(handlerResponse));
					return ResponseEntity.status(HttpStatus.BAD_REQUEST)
							.body(Utils.gson.toJsonTree(errorMessage).toString());
				}
			}
		}
	}
}
