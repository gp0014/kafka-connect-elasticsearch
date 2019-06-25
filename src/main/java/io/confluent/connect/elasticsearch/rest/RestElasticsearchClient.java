package io.confluent.connect.elasticsearch.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.*;
import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.confluent.connect.elasticsearch.bulk.BulkResponse;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.*;


public class RestElasticsearchClient implements ElasticsearchClient {

    private static final Logger log = LoggerFactory.getLogger(RestElasticsearchClient.class);

    private RestHighLevelClient client;

    private Version version = Version.ES_V7;

    private ObjectMapper mapper = new ObjectMapper();

    private final Set<String> indexCache = new HashSet<>();

    public RestElasticsearchClient(Map<String, String> props) {
        try {
            ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
            final int connTimeout = config.getInt(
                    ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
            final int readTimeout = config.getInt(
                    ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG);

            final String username = config.getString(
                    ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG);
            final Password password = config.getPassword(
                    ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG);
            List<String> address = config.getList(
                    ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);
            HttpHost[] hosts = new HttpHost[address.size()];
            for (int i = 0; i < address.size(); i++) {
                URL url = new URL(address.get(i));
                hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
            }
            RestClientBuilder builder = RestClient.builder(hosts);
            if (username != null) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(username, password == null ? null : password.value()));
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            builder.setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(connTimeout);
                return requestConfigBuilder;
            });
            client = new RestHighLevelClient(builder);
//            version = getServerVersion();
        } catch (Exception e) {
            log.error("Encountered configuration error when startup", e);
            throw new ConfigException(e.getLocalizedMessage());
        }
    }

//    private Version getServerVersion() {
//        // Default to newest version for forward compatibility
//        Version defaultVersion = Version.ES_V6;
//
//        try {
//            Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_nodes/_all/version"));
//            JsonNode result = mapper.readTree(response.getEntity().getContent());
//            checkForError(result);
//            Iterator<JsonNode> iterator = result.iterator();
//            if (!iterator.hasNext()) {
//                log.warn("Couldn't get Elasticsearch version, nodesRoot is null or empty");
//                return defaultVersion;
//            }
//            JsonNode node = iterator.next();
//            String esVersion = node.get("version").asText();
//            if (esVersion == null) {
//                log.warn("Couldn't get Elasticsearch version, version is null");
//                return defaultVersion;
//            } else if (esVersion.startsWith("1.")) {
//                return Version.ES_V1;
//            } else if (esVersion.startsWith("2.")) {
//                return Version.ES_V2;
//            } else if (esVersion.startsWith("5.")) {
//                return Version.ES_V5;
//            } else if (esVersion.startsWith("6.")) {
//                return Version.ES_V6;
//            } else if (esVersion.startsWith("7.")) {
//                return Version.ES_V7;
//            }
//        } catch (IOException e) {
//            log.error("Couldn't get Elasticsearch version", e);
//        }
//        return defaultVersion;
//    }

//    private void checkForError(JsonNode result) {
//        if (result.has("error") && result.get("error").isObject()) {
//            final JsonNode errorObject = result.get("error");
//            String errorType = errorObject.has("type") ? errorObject.get("type").asText() : "";
//            String errorReason = errorObject.has("reason") ? errorObject.get("reason").asText() : "";
//            throw new ConnectException("Couldn't connect to Elasticsearch, error: "
//                    + errorType + ", reason: " + errorReason);
//        }
//    }

    @Override
    public Version getVersion() {
        return this.version;
    }

    private boolean indexExists(String index) {
        if (indexCache.contains(index)) {
            return true;
        }
        try {
            return client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }


    @Override
    public void createIndices(Set<String> indices) {
        for (String index : indices) {
            if (!indexExists(index)) {
                IndicesClient indicesClient = client.indices();
                try {
                    CreateIndexResponse createIndexResponse = indicesClient.create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
                    if (!createIndexResponse.isShardsAcknowledged()) {
                        // Check if index was created by another client
                        if (!indexExists(index)) {
                            throw new ConnectException("Could not create index '" + index + "'");
                        }
                    }
                    indexCache.add(index);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        }
    }

    @Override
    public void createMapping(String index, String type, Schema schema) throws IOException {
        PutMappingRequest request = new PutMappingRequest(index);
//        String mapping;
//        if (getVersion() == Version.ES_V7) {
//            mapping = Mapping.inferMapping(this, schema).toString();
//            request.source(mapping, XContentType.JSON);
//        } else {
//            ObjectNode obj = JsonNodeFactory.instance.objectNode();
//            obj.set(type, Mapping.inferMapping(this, schema));
//            mapping = obj.toString();
//            request.source(mapping, XContentType.JSON);
//        }
        String mapping = Mapping.inferMapping(this, schema).toString();
        AcknowledgedResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new ConnectException("Cannot create mapping " + mapping + " -- ");
        }
    }

    @Override
    public String getMapping(String index, String type) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(index);
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        return mapper.writeValueAsString(response.mappings());
    }

    @Override
    public BulkRequest createBulkRequest(List<IndexableRecord> batch) {
        org.elasticsearch.action.bulk.BulkRequest request = new org.elasticsearch.action.bulk.BulkRequest();
        for (IndexableRecord record : batch) {
            request.add(toBulkableRequest(record));
        }
        return new RestBulkRequest(request);
    }

    // visible for testing
    protected DocWriteRequest toBulkableRequest(IndexableRecord record) {
        // If payload is null, the record was a tombstone and we should delete from the index.
        return record.payload != null ? toIndexRequest(record) : toDeleteRequest(record);
    }

    private DeleteRequest toDeleteRequest(IndexableRecord record) {
        DeleteRequest request = new DeleteRequest();
        request.id(record.key.id).index(record.key.index);
        // TODO: Should version information be set here?
        return request;
    }

    private IndexRequest toIndexRequest(IndexableRecord record) {
        IndexRequest request = new IndexRequest();
        request.source(record.payload, XContentType.JSON).index(record.key.index).id(record.key.id);

        if (record.version != null) {
            request.versionType(VersionType.EXTERNAL).version(record.version);
        }
        return request;
    }

    @Override
    public BulkResponse executeBulk(BulkRequest bulk) throws IOException {
        org.elasticsearch.action.bulk.BulkResponse responses = client.bulk(((RestBulkRequest) bulk).getBulk(), RequestOptions.DEFAULT);
        if (!responses.hasFailures()) {
            log.debug("Bulk request execute success");
            return BulkResponse.success();
        }

        boolean retryable = true;
        final List<Key> versionConflicts = new ArrayList<>();
        final List<String> errors = new ArrayList<>();

        for (BulkItemResponse itemResponse : responses.getItems()) {
            if (itemResponse.isFailed()) {
                RestStatus status = itemResponse.getFailure().getStatus();
                if (status == RestStatus.CONFLICT) {
                    versionConflicts.add(new Key(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId()));
                } else if(status == RestStatus.BAD_REQUEST) {
                    retryable = false;
                    errors.add(itemResponse.getFailureMessage());
                } else {
                    errors.add(itemResponse.getFailureMessage());
                }
            }
        }

        if (!versionConflicts.isEmpty()) {
            log.debug("Ignoring version conflicts for items: {}", versionConflicts);
            if (errors.isEmpty()) {
                // The only errors were version conflicts
                return BulkResponse.success();
            }
        }

        final String errorInfo = errors.isEmpty() ? responses.buildFailureMessage() : errors.stream().reduce((a, b) -> a + ", " + b).get();

        return BulkResponse.failure(retryable, errorInfo);
    }

    @Override
    public String search(String query, String index, String type) throws IOException {
        SearchRequest request = new SearchRequest();
        if (index != null) {
            request.indices(index);
        }
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        return response.toString();
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.error("Error occurred when close client.");
        }
    }
}
