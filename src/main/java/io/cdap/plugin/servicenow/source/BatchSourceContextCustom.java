package io.cdap.plugin.servicenow.source;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class MyFailureCollector implements FailureCollector {

    private final ArrayList<ValidationFailure> failures;

    MyFailureCollector() {
        failures = new ArrayList<>();
    }

    @Override
    public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
        ValidationFailure vf = new ValidationFailure(message);
        failures.add(vf);
        return vf;

    }

    @Override
    public ValidationException getOrThrowException() throws ValidationException {
//        if (!failures.isEmpty()) {
//            throw new ValidationException(failures);
//        }
        return new ValidationException(failures);
    }

    @Override
    public List<ValidationFailure> getValidationFailures() {
        return failures;
    }
}

    public class BatchSourceContextCustom implements BatchSourceContext {

        private String test;
//        private ArrayList<ValidationFailure> failures;
        private final MyFailureCollector myFailureCollector;

        BatchSourceContextCustom(String testValue) {
            test = testValue;
//            failures = new ArrayList<>();
            myFailureCollector = new MyFailureCollector();
        }

        @Override
        public void setInput(Input input) {

        }

        @Override
        public boolean isPreviewEnabled() {
            return false;
        }

        @Override
        public void createDataset(String datasetName, String typeName, DatasetProperties properties) {

        }

        @Override
        public boolean datasetExists(String datasetName) {
            return true;
        }

        @Override
        public String getStageName() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public String getPipelineName() {
            return null;
        }

        @Override
        public long getLogicalStartTime() {
            return 0;
        }

        @Override
        public StageMetrics getMetrics() {
            return null;
        }

        @Override
        public PluginProperties getPluginProperties() {
            return null;
        }

        @Override
        public PluginProperties getPluginProperties(String pluginId) {
            return null;
        }

        @Override
        public <T> Class<T> loadPluginClass(String pluginId) {
            return null;
        }

        @Override
        public <T> T newPluginInstance(String pluginId) throws InstantiationException {
            return null;
        }

        @Nullable
        @Override
        public Schema getInputSchema() {
            return null;
        }

        @Override
        public Map<String, Schema> getInputSchemas() {
            return null;
        }

        @Nullable
        @Override
        public Schema getOutputSchema() {
            return null;
        }

        @Override
        public Map<String, Schema> getOutputPortSchemas() {
            return null;
        }

        @Override
        public SettableArguments getArguments() {
            SettableArguments settableArguments = new SettableArgumentsCustom();
            return settableArguments;
        }

        @Override
        public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
            return null;
        }

        @Override
        public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
            return null;
        }

        @Override
        public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) throws DatasetInstantiationException {
            return null;
        }

        @Override
        public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments) throws DatasetInstantiationException {
            return null;
        }

        @Override
        public void releaseDataset(Dataset dataset) {

        }

        @Override
        public void discardDataset(Dataset dataset) {

        }

        @Override
        public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
            return null;
        }

        @Nullable
        @Override
        public URL getServiceURL(String applicationId, String serviceId) {
            return null;
        }

        @Nullable
        @Override
        public URL getServiceURL(String serviceId) {
            return null;
        }

        @Override
        public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
            return null;
        }

        @Override
        public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
            return null;
        }

        @Override
        public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {

        }

        @Override
        public void addTags(MetadataEntity metadataEntity, String... tags) {

        }

        @Override
        public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {

        }

        @Override
        public void removeMetadata(MetadataEntity metadataEntity) {

        }

        @Override
        public void removeProperties(MetadataEntity metadataEntity) {

        }

        @Override
        public void removeProperties(MetadataEntity metadataEntity, String... keys) {

        }

        @Override
        public void removeTags(MetadataEntity metadataEntity) {

        }

        @Override
        public void removeTags(MetadataEntity metadataEntity, String... tags) {

        }

        @Override
        public void record(List<FieldOperation> fieldOperations) {

        }

        @Override
        public FailureCollector getFailureCollector() {
            return myFailureCollector;
        }

        @Nullable
        @Override
        public URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
            return BatchSourceContext.super.getServiceURL(namespaceId, applicationId, serviceId);
        }
    }
