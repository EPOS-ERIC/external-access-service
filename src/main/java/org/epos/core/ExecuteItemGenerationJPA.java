package org.epos.core;

import abstractapis.AbstractAPI;
import commonapis.LinkedEntityAPI;
import metadataapis.EntityNames;
import org.epos.api.beans.Distribution;
import org.epos.api.beans.ServiceParameter;
import org.epos.api.utility.Utils;
import org.epos.eposdatamodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;


public class ExecuteItemGenerationJPA {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteItemGenerationJPA.class);

	public static Distribution generate(Map<String,Object> parameters) {

		LOGGER.info("Parameters {}", parameters);

		org.epos.eposdatamodel.Distribution distributionSelected =  (org.epos.eposdatamodel.Distribution) AbstractAPI.retrieveAPI(EntityNames.DISTRIBUTION.name()).retrieve(parameters.get("id").toString());

		if (distributionSelected == null) return null;

		DataProduct dp;
		if (distributionSelected.getDataProduct() != null &&
				!distributionSelected.getDataProduct().isEmpty()) {
			dp = (DataProduct) LinkedEntityAPI.retrieveFromLinkedEntity(distributionSelected.getDataProduct().get(0));
			if (dp == null) return null;
		} else {
			return null;
		}

		WebService ws = distributionSelected.getAccessService() != null && !distributionSelected.getAccessService().isEmpty() ?
				(WebService) LinkedEntityAPI.retrieveFromLinkedEntity(distributionSelected.getAccessService().get(0)) : null;
		if (ws == null && distributionSelected.getAccessService() != null) return null;

		Operation op = null;
		if (distributionSelected.getSupportedOperation() != null) {
			List<Operation> opList = distributionSelected.getSupportedOperation().parallelStream()
					.map(linkedEntity -> (Operation) LinkedEntityAPI.retrieveFromLinkedEntity(linkedEntity))
					.filter(Objects::nonNull).collect(Collectors.toList());
			op = !opList.isEmpty() ? opList.get(0) : null;
		}

		Distribution distribution = new Distribution();

		if (distributionSelected.getType() != null) {
			String[] type = distributionSelected.getType().split("\\/");
			distribution.setType(type[type.length - 1]);
		}

		distribution.setId(distributionSelected.getInstanceId());

		if (distributionSelected.getDownloadURL() != null) {
			distribution.setDownloadURL(
					Optional.of(
							String.join(".", distributionSelected.getDownloadURL())
					).orElse(null)
			);
		}

		if (distributionSelected.getAccessURL() != null) {
			distribution.setEndpoint(
					Optional.of(
							String.join(".", distributionSelected.getAccessURL())
					).orElse(null)
			);
		}

		distribution.setParameters(new ArrayList<>());
		// OPERATION AND PARAMETERS
		if (Objects.nonNull(op)) {
			distribution.setEndpoint(op.getTemplate());
			if (op.getTemplate() != null) distribution.setServiceEndpoint(op.getTemplate().split("\\{")[0]);
			distribution.setOperationid(op.getInstanceId());
			if (op.getMapping() != null && !op.getMapping().isEmpty()) {
				for (LinkedEntity mpLe : op.getMapping()) {
						Mapping mp = (Mapping) LinkedEntityAPI.retrieveFromLinkedEntity(mpLe);
						ServiceParameter sp = new ServiceParameter();
						sp.setDefaultValue(mp.getDefaultValue());
						sp.setEnumValue(
								mp.getParamValue() != null ?
										mp.getParamValue()
										: new ArrayList<>()
						);
						sp.setName(mp.getVariable());
						sp.setMaxValue(mp.getMaxValue());
						sp.setMinValue(mp.getMinValue());
						sp.setLabel(mp.getLabel() != null ? mp.getLabel().replaceAll("@en", "") : null);
						sp.setProperty(mp.getProperty());
						sp.setRequired(mp.getRequired()!=null? Boolean.parseBoolean(mp.getRequired()):null);
						sp.setType(mp.getRange() != null ? mp.getRange().replace("xsd:", "") : null);
						sp.setValue(null);
						if (parameters.containsKey("useDefaults") && Boolean.getBoolean(parameters.get("useDefaults").toString())) {
							if (mp.getDefaultValue() != null) {
								if (mp.getProperty() != null && mp.getValuePattern() != null) {
									if (mp.getProperty().equals("schema:startDate") || mp.getProperty().equals("schema:endDate")) {
										try {
											sp.setValue(Utils.convertDateUsingPattern(mp.getDefaultValue(), null, mp.getValuePattern()));
										} catch (ParseException e) {
											LOGGER.error(e.getLocalizedMessage());
										}
									}
								} else sp.setValue(mp.getDefaultValue());
							} else sp.setValue(null);
						} else {
							if (parameters.containsKey("params")) {
								JsonObject params = Utils.gson.fromJson(parameters.get("params").toString(), JsonObject.class);
								if (params.has(mp.getVariable()) && !params.get(mp.getVariable()).getAsString().isEmpty()) {
									if (mp.getProperty() != null && mp.getValuePattern() != null) {
										if (mp.getProperty().equals("schema:startDate") || mp.getProperty().equals("schema:endDate")) {
											try {
                                                String newDateParam = params.get(mp.getVariable()).getAsString().split(" ")[0];
												sp.setValue(Utils.convertDateUsingPattern(newDateParam, null, mp.getValuePattern()));
											} catch (ParseException e) {
												LOGGER.error(e.getLocalizedMessage());
											}
										}
									} else if (mp.getProperty() == null && mp.getValuePattern() != null) {
										if (Utils.checkStringPattern(params.get(mp.getVariable()).getAsString(), mp.getValuePattern()))
											sp.setValue(params.get(mp.getVariable()).getAsString());
										else if (!Utils.checkStringPattern(params.get(mp.getVariable()).getAsString(), mp.getValuePattern()) && Utils.checkStringPatternSingleQuotes(mp.getValuePattern()))
											sp.setValue("'" + params.get(mp.getVariable()).getAsString() + "'");
										else
											sp.setValue(params.get(mp.getVariable()).getAsString()); //return new JsonObject();
									} else sp.setValue(params.get(mp.getVariable()).getAsString());
								} else sp.setValue(null);
							}
						}
						if (sp.getValue() != null) {
                            sp.setValue(URLEncoder.encode(sp.getValue(), StandardCharsets.UTF_8));
                        }

						sp.setValuePattern(mp.getValuePattern());
						sp.setVersion(null);
						sp.setReadOnlyValue(mp.getReadOnlyValue());
						sp.setMultipleValue(mp.getMultipleValues());
						distribution.getParameters().add(sp);

				}
			}
		}

		return distribution;
	}

}
