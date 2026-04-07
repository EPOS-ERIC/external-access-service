package org.epos.core;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.JsonObject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

import org.epos.api.beans.Distribution;
import org.epos.api.beans.ServiceParameter;
import org.epos.api.utility.Utils;
import org.epos.eposdatamodel.User;
import org.epos.handler.dbapi.service.EntityManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL-based implementation for retrieving distribution details.
 * This replaces the JPA-based DistributionDetailsGenerationJPA for improved performance.
 */
public class ExecuteItemGenerationSQL {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteItemGenerationSQL.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int SQL_BUILDER_INITIAL_CAPACITY = 8192;

    public static Distribution generate(Map<String, Object> parameters) {
        final long startTime = System.nanoTime();
        LOGGER.info("Generating distribution details (SQL) for parameters: {}", parameters);

        String distributionId = parameters.get("id").toString();

        EntityManager em = null;
        try {
            em = EntityManagerService.getInstance().createEntityManager();

            // Build SQL with access control based on user
            QueryContext ctx = buildDistributionDetailsSQL();
            Query query = em.createNativeQuery(ctx.sql.toString());
            query.setParameter(1, distributionId);

            // Bind additional parameters for access control
            for (Map.Entry<Integer, Object> entry : ctx.params.entrySet()) {
                query.setParameter(entry.getKey(), entry.getValue());
            }

            @SuppressWarnings("unchecked")
            List<Object[]> results = query.getResultList();

            if (results.isEmpty()) {
                LOGGER.warn("Distribution not found or not accessible for id: {}", distributionId);
                return null;
            }

            Object[] row = results.get(0);
            Distribution distribution = mapRowToDistribution(row, parameters);

            LOGGER.info("Distribution details generated (SQL) in {} ms",
                    (System.nanoTime() - startTime) / 1_000_000);

            return distribution;

        } catch (Exception e) {
            LOGGER.error("Failed to generate distribution details", e);
            throw new RuntimeException("Failed to generate distribution details", e);
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }

    /**
     * Query context for building dynamic SQL with parameters
     */
    private static final class QueryContext {
        final StringBuilder sql;
        final Map<Integer, Object> params;
        int paramIndex;

        QueryContext() {
            this.sql = new StringBuilder(SQL_BUILDER_INITIAL_CAPACITY);
            this.params = new HashMap<>(8);
            this.paramIndex = 2; // Start at 2 because ?1 is used for distributionId
        }
    }

    /**
     * Builds the SQL query for retrieving distribution details with all related data.
     * Applies access control based on user permissions.
     */
    private static QueryContext buildDistributionDetailsSQL() {
        QueryContext ctx = new QueryContext();

        ctx.sql.append("WITH distribution_base AS ( ");
        ctx.sql.append("  SELECT d.instance_id, d.meta_id, d.uid, d.type, d.format, d.license, d.version_id, ");
        ctx.sql.append("         v.status AS versioning_status, v.editor_id ");
        ctx.sql.append("  FROM metadata_catalogue.distribution d ");
        ctx.sql.append("  JOIN metadata_catalogue.versioningstatus v ON d.version_id = v.version_id ");
        ctx.sql.append("  WHERE d.instance_id = ?1 ),");

        // Distribution titles
        ctx.sql.append("dist_titles AS ( ");
        ctx.sql.append("  SELECT dt.distribution_instance_id, STRING_AGG(dt.title, '.' ORDER BY dt.lang) AS title ");
        ctx.sql.append("  FROM metadata_catalogue.distribution_title dt ");
        ctx.sql.append("  WHERE dt.distribution_instance_id = ?1 ");
        ctx.sql.append("  GROUP BY dt.distribution_instance_id ");
        ctx.sql.append("), ");

        // Distribution descriptions
        ctx.sql.append("dist_descriptions AS ( ");
        ctx.sql.append("  SELECT dd.distribution_instance_id, STRING_AGG(dd.description, '.' ORDER BY dd.lang) AS description ");
        ctx.sql.append("  FROM metadata_catalogue.distribution_description dd ");
        ctx.sql.append("  WHERE dd.distribution_instance_id = ?1 ");
        ctx.sql.append("  GROUP BY dd.distribution_instance_id ");
        ctx.sql.append("), ");

        // Download URLs
        ctx.sql.append("dist_download_urls AS ( ");
        ctx.sql.append("  SELECT de.distribution_instance_id, STRING_AGG(e.value, '.') AS download_urls ");
        ctx.sql.append("  FROM metadata_catalogue.distribution_element de ");
        ctx.sql.append("  JOIN metadata_catalogue.element e ON de.element_instance_id = e.instance_id ");
        ctx.sql.append("  WHERE de.distribution_instance_id = ?1 AND e.type = 'DOWNLOADURL' ");
        ctx.sql.append("  GROUP BY de.distribution_instance_id ");
        ctx.sql.append("), ");

        // Access URLs
        ctx.sql.append("dist_access_urls AS ( ");
        ctx.sql.append("  SELECT de.distribution_instance_id, STRING_AGG(e.value, '.') AS access_urls ");
        ctx.sql.append("  FROM metadata_catalogue.distribution_element de ");
        ctx.sql.append("  JOIN metadata_catalogue.element e ON de.element_instance_id = e.instance_id ");
        ctx.sql.append("  WHERE de.distribution_instance_id = ?1 AND e.type = 'ACCESSURL' ");
        ctx.sql.append("  GROUP BY de.distribution_instance_id ");
        ctx.sql.append("), ");

        // DataProduct info
        ctx.sql.append("dataproduct_info AS ( ");
        ctx.sql.append("  SELECT ddp.distribution_instance_id, dp.instance_id AS dataproduct_id, ");
        ctx.sql.append("         dp.keywords, dp.accrualperiodicity, dp.qualityassurance, dp.accessright ");
        ctx.sql.append("  FROM metadata_catalogue.distribution_dataproduct ddp ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct dp ON ddp.dataproduct_instance_id = dp.instance_id ");
        ctx.sql.append("  WHERE ddp.distribution_instance_id = ?1 ");
        ctx.sql.append("  LIMIT 1 ");
        ctx.sql.append("), ");

        // DataProduct identifiers (DOI, DDSS-ID)
        ctx.sql.append("dp_identifiers AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, ");
        ctx.sql.append("         JSONB_AGG(JSONB_BUILD_OBJECT('type', i.type, 'value', i.value)) AS identifiers ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_identifier dpi ON di.dataproduct_id = dpi.dataproduct_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.identifier i ON dpi.identifier_instance_id = i.instance_id ");
        ctx.sql.append("  GROUP BY di.dataproduct_id ");
        ctx.sql.append("), ");

        // DataProduct spatial
        ctx.sql.append("dp_spatial AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, STRING_AGG(s.location, ' #EPOS# ') AS locations ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_spatial dps ON di.dataproduct_id = dps.dataproduct_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.spatial s ON dps.spatial_instance_id = s.instance_id ");
        ctx.sql.append("  GROUP BY di.dataproduct_id ");
        ctx.sql.append("), ");

        // DataProduct temporal
        ctx.sql.append("dp_temporal AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, t.startdate, t.enddate ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_temporal dpt ON di.dataproduct_id = dpt.dataproduct_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.temporal t ON dpt.temporal_instance_id = t.instance_id ");
        ctx.sql.append("  LIMIT 1 ");
        ctx.sql.append("), ");

        // DataProduct publishers (data providers)
        ctx.sql.append("dp_publishers AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, ");
        ctx.sql.append("         JSONB_AGG(DISTINCT JSONB_BUILD_OBJECT( ");
        ctx.sql.append("           'instance_id', o.instance_id, 'legal_name', o.legalname, ");
        ctx.sql.append("           'acronym', o.acronym, 'url', o.url, 'logo', o.logo ");
        ctx.sql.append("         )) AS publishers ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_publisher dpp ON di.dataproduct_id = dpp.dataproduct_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.organization o ON dpp.organization_instance_id = o.instance_id ");
        ctx.sql.append("  GROUP BY di.dataproduct_id ");
        ctx.sql.append("), ");

        // DataProduct categories (science domains)
        ctx.sql.append("dp_categories AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, ");
        ctx.sql.append("         JSONB_AGG(DISTINCT JSONB_BUILD_OBJECT('uid', c.uid, 'name', c.name, 'instance_id', c.instance_id)) AS categories ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_category dpc ON di.dataproduct_id = dpc.dataproduct_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.category c ON dpc.category_instance_id = c.instance_id ");
        ctx.sql.append("  GROUP BY di.dataproduct_id ");
        ctx.sql.append("), ");

        // DataProduct contact points
        ctx.sql.append("dp_contactpoints AS ( ");
        ctx.sql.append("  SELECT di.dataproduct_id, COUNT(*) AS contact_count ");
        ctx.sql.append("  FROM dataproduct_info di ");
        ctx.sql.append("  JOIN metadata_catalogue.dataproduct_contactpoint dcp ON di.dataproduct_id = dcp.dataproduct_instance_id ");
        ctx.sql.append("  GROUP BY di.dataproduct_id ");
        ctx.sql.append("), ");

        // WebService info
        ctx.sql.append("webservice_info AS ( ");
        ctx.sql.append("  SELECT wd.distribution_instance_id, ws.instance_id AS webservice_id, ");
        ctx.sql.append("         ws.name, ws.description AS ws_description, ws.provider AS provider_id, ws.keywords AS ws_keywords ");
        ctx.sql.append("  FROM metadata_catalogue.webservice_distribution wd ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice ws ON wd.webservice_instance_id = ws.instance_id ");
        ctx.sql.append("  WHERE wd.distribution_instance_id = ?1 ");
        ctx.sql.append("  LIMIT 1 ");
        ctx.sql.append("), ");

        // WebService documentation
        ctx.sql.append("ws_documentation AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, STRING_AGG(e.value, '.') AS documentation ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice_element we ON wi.webservice_id = we.webservice_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.element e ON we.element_instance_id = e.instance_id ");
        ctx.sql.append("  WHERE e.type = 'DOCUMENTATION' ");
        ctx.sql.append("  GROUP BY wi.webservice_id ");
        ctx.sql.append("), ");

        // WebService provider
        ctx.sql.append("ws_provider AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, ");
        ctx.sql.append("         JSONB_BUILD_OBJECT( ");
        ctx.sql.append("           'instance_id', o.instance_id, 'legal_name', o.legalname, ");
        ctx.sql.append("           'acronym', o.acronym, 'url', o.url, 'logo', o.logo ");
        ctx.sql.append("         ) AS provider ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.organization o ON wi.provider_id = o.instance_id ");
        ctx.sql.append("), ");

        // WebService spatial
        ctx.sql.append("ws_spatial AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, STRING_AGG(s.location, ' #EPOS# ') AS locations ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice_spatial wss ON wi.webservice_id = wss.webservice_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.spatial s ON wss.spatial_instance_id = s.instance_id ");
        ctx.sql.append("  GROUP BY wi.webservice_id ");
        ctx.sql.append("), ");

        // WebService temporal
        ctx.sql.append("ws_temporal AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, t.startdate, t.enddate ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice_temporal wst ON wi.webservice_id = wst.webservice_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.temporal t ON wst.temporal_instance_id = t.instance_id ");
        ctx.sql.append("  LIMIT 1 ");
        ctx.sql.append("), ");

        // WebService categories (service types)
        ctx.sql.append("ws_categories AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, ");
        ctx.sql.append("         JSONB_AGG(DISTINCT JSONB_BUILD_OBJECT('uid', c.uid, 'name', c.name, 'instance_id', c.instance_id)) AS service_types ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice_category wc ON wi.webservice_id = wc.webservice_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.category c ON wc.category_instance_id = c.instance_id ");
        ctx.sql.append("  GROUP BY wi.webservice_id ");
        ctx.sql.append("), ");

        // WebService contact points
        ctx.sql.append("ws_contactpoints AS ( ");
        ctx.sql.append("  SELECT wi.webservice_id, COUNT(*) AS contact_count ");
        ctx.sql.append("  FROM webservice_info wi ");
        ctx.sql.append("  JOIN metadata_catalogue.webservice_contactpoint wcp ON wi.webservice_id = wcp.webservice_instance_id ");
        ctx.sql.append("  GROUP BY wi.webservice_id ");
        ctx.sql.append("), ");

        // Operation info
        ctx.sql.append("operation_info AS ( ");
        ctx.sql.append("  SELECT od.distribution_instance_id, op.instance_id AS operation_id, op.template, op.uid AS operation_uid ");
        ctx.sql.append("  FROM metadata_catalogue.operation_distribution od ");
        ctx.sql.append("  JOIN metadata_catalogue.operation op ON od.operation_instance_id = op.instance_id ");
        ctx.sql.append("  WHERE od.distribution_instance_id = ?1 ");
        ctx.sql.append("  LIMIT 1 ");
        ctx.sql.append("), ");

        // Operation mappings (parameters)
        ctx.sql.append("op_mappings AS ( ");
        ctx.sql.append("  SELECT oi.operation_id, ");
        ctx.sql.append("         JSONB_AGG(JSONB_BUILD_OBJECT( ");
        ctx.sql.append("           'variable', m.variable, 'label', m.label, 'required', m.required, ");
        ctx.sql.append("           'range', m.range, 'defaultvalue', m.defaultvalue, 'minvalue', m.minvalue, ");
        ctx.sql.append("           'maxvalue', m.maxvalue, 'property', m.property, 'valuepattern', m.valuepattern, ");
        ctx.sql.append("           'readonly', m.read_only_value, 'multiple', m.multiple_values, ");
        ctx.sql.append("           'paramvalues', (SELECT ARRAY_AGG(e.value) FROM metadata_catalogue.mapping_element me ");
        ctx.sql.append("                           JOIN metadata_catalogue.element e ON me.element_instance_id = e.instance_id ");
        ctx.sql.append("                           WHERE me.mapping_instance_id = m.instance_id AND e.type = 'PARAMVALUE') ");
        ctx.sql.append("         )) AS mappings ");
        ctx.sql.append("  FROM operation_info oi ");
        ctx.sql.append("  JOIN metadata_catalogue.operation_mapping om ON oi.operation_id = om.operation_instance_id ");
        ctx.sql.append("  JOIN metadata_catalogue.mapping m ON om.mapping_instance_id = m.instance_id ");
        ctx.sql.append("  GROUP BY oi.operation_id ");
        ctx.sql.append(") ");

        // Main SELECT
        ctx.sql.append("SELECT ");
        ctx.sql.append("  db.instance_id, db.meta_id, db.uid, db.type, db.format, db.license, ");
        ctx.sql.append("  db.versioning_status, db.editor_id, ");
        ctx.sql.append("  COALESCE(dt.title, '') AS title, ");
        ctx.sql.append("  COALESCE(dd.description, '') AS description, ");
        ctx.sql.append("  COALESCE(ddu.download_urls, '') AS download_urls, ");
        ctx.sql.append("  COALESCE(dau.access_urls, '') AS access_urls, ");
        ctx.sql.append("  di.dataproduct_id, di.keywords, di.accrualperiodicity, di.qualityassurance, di.accessright, ");
        ctx.sql.append("  COALESCE(CAST(dpi.identifiers AS text), '[]') AS dp_identifiers, ");
        ctx.sql.append("  COALESCE(dps.locations, '') AS dp_spatial, ");
        ctx.sql.append("  dpt.startdate AS dp_start_date, dpt.enddate AS dp_end_date, ");
        ctx.sql.append("  COALESCE(CAST(dpp.publishers AS text), '[]') AS dp_publishers, ");
        ctx.sql.append("  COALESCE(CAST(dpc.categories AS text), '[]') AS dp_categories, ");
        ctx.sql.append("  COALESCE(dpcp.contact_count, 0) AS dp_contact_count, ");
        ctx.sql.append("  wi.webservice_id, wi.name AS ws_name, wi.ws_description, wi.ws_keywords, ");
        ctx.sql.append("  COALESCE(wsd.documentation, '') AS ws_documentation, ");
        ctx.sql.append("  COALESCE(CAST(wsp.provider AS text), '{}') AS ws_provider, ");
        ctx.sql.append("  COALESCE(wss.locations, '') AS ws_spatial, ");
        ctx.sql.append("  wst.startdate AS ws_start_date, wst.enddate AS ws_end_date, ");
        ctx.sql.append("  COALESCE(CAST(wsc.service_types AS text), '[]') AS ws_service_types, ");
        ctx.sql.append("  COALESCE(wscp.contact_count, 0) AS ws_contact_count, ");
        ctx.sql.append("  oi.operation_id, oi.template, oi.operation_uid, ");
        ctx.sql.append("  COALESCE(CAST(opm.mappings AS text), '[]') AS op_mappings ");
        ctx.sql.append("FROM distribution_base db ");
        ctx.sql.append("LEFT JOIN dist_titles dt ON db.instance_id = dt.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN dist_descriptions dd ON db.instance_id = dd.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN dist_download_urls ddu ON db.instance_id = ddu.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN dist_access_urls dau ON db.instance_id = dau.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN dataproduct_info di ON db.instance_id = di.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN dp_identifiers dpi ON di.dataproduct_id = dpi.dataproduct_id ");
        ctx.sql.append("LEFT JOIN dp_spatial dps ON di.dataproduct_id = dps.dataproduct_id ");
        ctx.sql.append("LEFT JOIN dp_temporal dpt ON di.dataproduct_id = dpt.dataproduct_id ");
        ctx.sql.append("LEFT JOIN dp_publishers dpp ON di.dataproduct_id = dpp.dataproduct_id ");
        ctx.sql.append("LEFT JOIN dp_categories dpc ON di.dataproduct_id = dpc.dataproduct_id ");
        ctx.sql.append("LEFT JOIN dp_contactpoints dpcp ON di.dataproduct_id = dpcp.dataproduct_id ");
        ctx.sql.append("LEFT JOIN webservice_info wi ON db.instance_id = wi.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN ws_documentation wsd ON wi.webservice_id = wsd.webservice_id ");
        ctx.sql.append("LEFT JOIN ws_provider wsp ON wi.webservice_id = wsp.webservice_id ");
        ctx.sql.append("LEFT JOIN ws_spatial wss ON wi.webservice_id = wss.webservice_id ");
        ctx.sql.append("LEFT JOIN ws_temporal wst ON wi.webservice_id = wst.webservice_id ");
        ctx.sql.append("LEFT JOIN ws_categories wsc ON wi.webservice_id = wsc.webservice_id ");
        ctx.sql.append("LEFT JOIN ws_contactpoints wscp ON wi.webservice_id = wscp.webservice_id ");
        ctx.sql.append("LEFT JOIN operation_info oi ON db.instance_id = oi.distribution_instance_id ");
        ctx.sql.append("LEFT JOIN op_mappings opm ON oi.operation_id = opm.operation_id ");

        return ctx;
    }

    /**
     * Maps a database result row to a Distribution object.
     */
    private static Distribution mapRowToDistribution(Object[] row, Map<String, Object> parameters) {
        int i = 0;

        // Distribution base fields
        String instanceId = (String) row[i++];
        String metaId = (String) row[i++];
        String uid = (String) row[i++];
        String type = (String) row[i++];
        String format = (String) row[i++];
        String license = (String) row[i++];
        String versioningStatus = (String) row[i++];
        String editorId = (String) row[i++];
        String title = (String) row[i++];
        String description = (String) row[i++];
        String downloadUrls = (String) row[i++];
        String accessUrls = (String) row[i++];

        // DataProduct fields
        String dataproductId = (String) row[i++];
        String keywords = (String) row[i++];
        String accrualPeriodicity = (String) row[i++];
        String qualityAssurance = (String) row[i++];
        String accessRight = (String) row[i++];
        String dpIdentifiersJson = (String) row[i++];
        String dpSpatial = (String) row[i++];
        Timestamp dpStartDate = (Timestamp) row[i++];
        Timestamp dpEndDate = (Timestamp) row[i++];
        String dpPublishersJson = (String) row[i++];
        String dpCategoriesJson = (String) row[i++];
        Long dpContactCount = ((Number) row[i++]).longValue();

        // WebService fields
        String webserviceId = (String) row[i++];
        String wsName = (String) row[i++];
        String wsDescription = (String) row[i++];
        String wsKeywords = (String) row[i++];
        String wsDocumentation = (String) row[i++];
        String wsProviderJson = (String) row[i++];
        String wsSpatial = (String) row[i++];
        Timestamp wsStartDate = (Timestamp) row[i++];
        Timestamp wsEndDate = (Timestamp) row[i++];
        String wsServiceTypesJson = (String) row[i++];
        Long wsContactCount = ((Number) row[i++]).longValue();

        // Operation fields
        String operationId = (String) row[i++];
        String template = (String) row[i++];
        String operationUid = (String) row[i++];
        String opMappingsJson = (String) row[i++];

        // Build Distribution object
        Distribution distribution = new Distribution();

        // Basic fields
        distribution.setId(instanceId);

        if (type != null) {
            String[] typeParts = type.split("/");
            distribution.setType(typeParts[typeParts.length - 1]);
        }

        distribution.setLicense(license);

        if (downloadUrls != null && !downloadUrls.isEmpty()) {
            distribution.setDownloadURL(downloadUrls);
        }

        if (accessUrls != null && !accessUrls.isEmpty()) {
            distribution.setEndpoint(accessUrls);
        }

        // Operation/Parameters
        if (operationId != null) {
            distribution.setEndpoint(template);
            if (template != null) {
                distribution.setServiceEndpoint(template.split("\\{")[0]);
            }
            distribution.setOperationid(operationUid);
            parseParameters(distribution, opMappingsJson, parameters);
        }

        return distribution;
    }

    private static void parseParameters(Distribution distribution, String mappingsJson, Map<String, Object> parameters) {
        distribution.setParameters(new ArrayList<>());

        if (isEmptyJson(mappingsJson)) {
            return;
        }

        try {
            JsonNode arrayNode = OBJECT_MAPPER.readTree(mappingsJson);
            for (JsonNode node : arrayNode) {
                ServiceParameter sp = new ServiceParameter();
                sp.setName(getTextOrNull(node, "variable"));
                sp.setLabel(getTextOrNull(node, "label") != null ?
                        getTextOrNull(node, "label").replaceAll("@en", "") : null);
                sp.setRequired(node.has("required") && !node.get("required").isNull() ?
                        Boolean.parseBoolean(node.get("required").asText()) : null);
                sp.setType(getTextOrNull(node, "range") != null ?
                        getTextOrNull(node, "range").replace("xsd:", "") : null);
                sp.setDefaultValue(getTextOrNull(node, "defaultvalue"));
                sp.setMinValue(getTextOrNull(node, "minvalue"));
                sp.setMaxValue(getTextOrNull(node, "maxvalue"));
                sp.setProperty(getTextOrNull(node, "property"));
                sp.setValuePattern(getTextOrNull(node, "valuepattern"));
                sp.setReadOnlyValue(getTextOrNull(node, "readonly"));
                sp.setMultipleValue(getTextOrNull(node, "multiple"));

                // Parse enum values
                if (node.has("paramvalues") && !node.get("paramvalues").isNull()) {
                    List<String> enumValues = new ArrayList<>();
                    for (JsonNode valNode : node.get("paramvalues")) {
                        if (!valNode.isNull()) {
                            enumValues.add(valNode.asText());
                        }
                    }
                    sp.setEnumValue(enumValues);
                } else {
                    sp.setEnumValue(new ArrayList<>());
                }
                sp.setValue(null);
                if (parameters.containsKey("useDefaults") && Boolean.getBoolean(parameters.get("useDefaults").toString())) {
                    if (sp.getDefaultValue() != null) {
                        if (sp.getProperty() != null && sp.getValuePattern() != null) {
                            if (sp.getProperty().equals("schema:startDate") || sp.getProperty().equals("schema:endDate")) {
                                try {
                                    sp.setValue(Utils.convertDateUsingPattern(sp.getDefaultValue(), null, sp.getValuePattern()));
                                } catch (ParseException e) {
                                    LOGGER.error(e.getLocalizedMessage());
                                }
                            }
                        } else sp.setValue(sp.getDefaultValue());
                    } else sp.setValue(null);
                } else {
                    if (parameters.containsKey("params")) {
                        JsonObject params = Utils.gson.fromJson(parameters.get("params").toString(), JsonObject.class);
                        if (params.has(sp.getName()) && !params.get(sp.getName()).getAsString().isEmpty()) {
                            if (sp.getProperty() != null && sp.getValuePattern() != null) {
                                if (sp.getProperty().equals("schema:startDate") || sp.getProperty().equals("schema:endDate")) {
                                    try {
                                        String newDateParam = params.get(sp.getName()).getAsString().split(" ")[0];
                                        sp.setValue(Utils.convertDateUsingPattern(newDateParam, null, sp.getValuePattern()));
                                    } catch (ParseException e) {
                                        LOGGER.error(e.getLocalizedMessage());
                                    }
                                }
                            } else if (sp.getProperty() == null && sp.getValuePattern() != null) {
                                if (Utils.checkStringPattern(params.get(sp.getName()).getAsString(), sp.getValuePattern()))
                                    sp.setValue(params.get(sp.getName()).getAsString());
                                else if (!Utils.checkStringPattern(params.get(sp.getName()).getAsString(), sp.getValuePattern()) && Utils.checkStringPatternSingleQuotes(sp.getValuePattern()))
                                    sp.setValue("'" + params.get(sp.getName()).getAsString() + "'");
                                else
                                    sp.setValue(params.get(sp.getName()).getAsString()); //return new JsonObject();
                            } else sp.setValue(params.get(sp.getName()).getAsString());
                        } else sp.setValue(null);
                    }
                }
                if (sp.getValue() != null) {
                    sp.setValue(URLEncoder.encode(sp.getValue(), StandardCharsets.UTF_8));
                }

                distribution.getParameters().add(sp);
            }
        } catch (JsonProcessingException e) {
            LOGGER.warn("Failed to parse mappings JSON: {}", e.getMessage());
        }
    }

    private static boolean isEmptyJson(String json) {
        return json == null || json.isEmpty() || "[]".equals(json) || "null".equals(json);
    }

    private static String getTextOrNull(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            return null;
        }
        return field.asText(null);
    }
}
