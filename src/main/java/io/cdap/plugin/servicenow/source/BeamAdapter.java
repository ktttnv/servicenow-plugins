package io.cdap.plugin.servicenow.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import com.google.gson.Gson;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

import javax.annotation.Nullable;
import java.io.Serializable;

class MyServiceNowSourceConfig extends ServiceNowSourceConfig {

    /**
     * Constructor for ServiceNowSourceConfig object.
     *
     * @param referenceName   The reference name
     * @param queryMode       The query mode
     * @param applicationName The application name
     * @param tableNameField  The field name to hold the table name value
     * @param tableName       The table name
     * @param clientId        The Client Id for ServiceNow
     * @param clientSecret    The Client Secret for ServiceNow
     * @param restApiEndpoint The rest API endpoint for ServiceNow
     * @param user            The user id for ServiceNow
     * @param password        The password for ServiceNow
     * @param valueType       The value type
     * @param startDate       The start date
     * @param endDate         The end date
     */
    public MyServiceNowSourceConfig(String referenceName, String queryMode, @Nullable String applicationName, @Nullable String tableNameField, @Nullable String tableName, String clientId, String clientSecret, String restApiEndpoint, String user, String password, String valueType, @Nullable String startDate, @Nullable String endDate) {
        super(referenceName, queryMode, applicationName, tableNameField, tableName, clientId, clientSecret, restApiEndpoint, user, password, valueType, startDate, endDate);
    }

    @Override
    public boolean containsMacro(String fieldName) {
        return false;
    }
}

public class BeamAdapter {



    public static void main(String[] args) throws Exception {
        Gson gson = new Gson();
        Configuration myHadoopConfiguration = new Configuration(false);

        String referenceName = "refname";
//        String clientId = "ekaterina.tatanova@akvelon.com";
        String clientId = "9b842c3fbb8001107883660ac9f84569";
//        String clientSecret = "R6zrwUK4Eqe$2!w";
        String clientSecret = "sQy{NC5n^2'YY\\zV";
//        String restApiEndpoint = "https://dev101840.service-now.com";
        String restApiEndpoint = "https://dev80682.service-now.com";
        String user = "admin";
//        String password = "qqikB6KBbZW5";
        String password = "aRer4S7TEwwJ";
        String queryMode = "Table";
//        String applicationName = "MyFirstApp";
        String applicationName = "SecondApp";
//        String tableNameField = "tablename";
        String tableNameField = "first_name";
        String tableName = "x_730267_secondapp_people";
        String valueType = "Actual";
//        String startDate = "";
//        String endDate = "";

//        MyServiceNowSourceConfig serviceNowSourceConfig = gson.fromJson(
//                String.format("{\"referenceName\":\"%s\",\"clientId\":\"%s\",\"clientSecret\":\"%s\",\"restApiEndpoint\":\"%s\",\"user\":\"%s\",\"password\":\"%s\",\"queryMode\":\"%s\",\"applicationName\":\"%s\",\"tableNameField\":\"%s\",\"tableName\":\"%s\",\"valueType\":\"%s\"}",
//                        referenceName,
//                        clientId,
//                        clientSecret,
//                        restApiEndpoint,
//                        user,
//                        password,
//                        queryMode,
//                        applicationName,
//                        tableNameField,
//                        tableName,
//                        valueType
//                ),
//                MyServiceNowSourceConfig.class);

        ServiceNowSourceConfig serviceNowSourceConfig = new MyServiceNowSourceConfig(
                referenceName,
                queryMode,
                applicationName,
                tableNameField,
                tableName,
                clientId,
                clientSecret,
                restApiEndpoint,
                user,
                password,
                valueType,
        null,
        null
        );

//        SourceInputFormatProvider serviceNowFormatProvider =
//                new SourceInputFormatProvider(ServiceNowInputFormat.class.getName(), myHadoopConfiguration);

//        final String PROPERTY_CONFIG_JSON = "cdap.servicenow.config";


        ServiceNowSource serviceNowSource = new ServiceNowSource(serviceNowSourceConfig);
        BatchSourceContext ctx = new BatchSourceContextCustom("myTest");

        serviceNowSource.prepareRun(ctx);
        ctx.getFailureCollector().getValidationFailures().
                forEach(vf -> {
                    System.out.println(vf.getMessage() + "\n" + vf.getCorrectiveAction());
                    System.out.println("Causes list:");
                    vf.getCauses().forEach(cause -> {
                        cause.getAttributes().keySet().forEach(key -> System.out.printf("%s: %s%n", key, cause.getAttribute(key)));
                    });
                });

        myHadoopConfiguration.setClass("mapreduce.job.inputformat.class", ServiceNowInputFormat.class,
                InputFormat.class);
        myHadoopConfiguration.setClass("key.class", Text.class, Object.class);
        myHadoopConfiguration.setClass("value.class", StructuredRecord.class, Object.class);
        myHadoopConfiguration.set("servicenow.plugin.conf", gson.toJson(serviceNowSourceConfig));

        Pipeline p = Pipeline.create();

        SimpleFunction<StructuredRecord, MyRecord> myOutputValueType =
                new SimpleFunction<StructuredRecord, MyRecord>() {
                    public MyRecord apply(StructuredRecord input) {
                        // ...logic to transform InputFormatValueClass to MyValueClass
                        return new MyRecord(input.getSchema().toString());
                    }
                };

        // Read data only with Hadoop configuration.
        PCollection<KV<Text, MyRecord>> pcol = p.apply("read",
                HadoopFormatIO.<Text, MyRecord>read()
                        .withConfiguration(myHadoopConfiguration)
                        .withValueTranslation(myOutputValueType)
        ).setCoder(
                KvCoder.of(
                        NullableCoder.of(WritableCoder.of(Text.class)),
                        SerializableCoder.of(MyRecord.class)
                ));
//
//        PCollection<String> strings = pcol.apply(MapElements
//                        .into(TypeDescriptors.strings())
//                        .via(
//                                ((SerializableFunction<KV<Text, Commit>, String>) input -> {
//                                    Gson gson1 = new Gson();
//                                    return gson1.toJson(input.getValue());
//                                })
//                        )
//                )
//                .setCoder(StringUtf8Coder.of());
//
//        strings.apply(TextIO.write().to("./txt.txt"));



        p.run();
    }
}
