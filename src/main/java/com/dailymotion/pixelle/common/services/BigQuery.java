package com.dailymotion.pixelle.common.services;

import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Table;
import org.slf4j.Logger;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import static com.dailymotion.pixelle.common.services.FileLoader.getTable;
import static com.dailymotion.pixelle.de.processor.DeHelper.CATEGORIESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.DEVICESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.EVENTSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMATSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.LANGUAGEBYCOUNTRY;
import static com.google.api.client.googleapis.auth.oauth2.GoogleCredential.fromStream;
import static com.google.api.services.bigquery.Bigquery.Builder;
import static com.google.api.services.bigquery.BigqueryScopes.BIGQUERY;
import static com.google.common.collect.HashBasedTable.create;
import static java.lang.Long.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static net.logstash.logback.encoder.org.apache.commons.lang.StringUtils.lowerCase;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.slf4j.LoggerFactory.getLogger;


public class BigQuery {
    // Static variables for API scope, callback URI, and HTTP/JSON functions
    private static final String PROJECT_ID = "dailymotion-pixelle";
    private static final List<String> SCOPES = asList(BIGQUERY);
    private static final String TOTAL = "total";
    private static final String BQ_APP_NAME = "pixelle-forecast";
    private static final String BQ_CREDENTIAL_FILENAME = "/bq.json";
    private static final String BQ_DONE_STATE = "DONE";
    private static final int BQ_PAUSE_TIME = 1000;


    /**
     * Global instances of HTTP transport and JSON factory objects.
     */
    private static final HttpTransport TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static Logger logger = getLogger(BigQuery.class);

    public static Table<String, String, Long> getCountryCountTable(String target) throws ForecastException {
        Bigquery bigquery = null;
        try {
            bigquery = createAuthorizedClient();
        } catch (IOException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        } catch (GeneralSecurityException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
        String query = null;

        if (target.equalsIgnoreCase(EVENTSBYCOUNTRY)) {
            query = "select visitor_country, event, count(*) as count from events.last_3_months where event is not null and req_type == 'promoted' and (event == 'opportunity' or event == 'view') and visitor_country is not null and (occurrence is null or occurrence == 1) and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by event, visitor_country";
        } else if (target.equalsIgnoreCase(LANGUAGEBYCOUNTRY)) {
            query = "select visitor_country,visitor_language,count(*) as count from events.last_3_months where visitor_country is not null and req_type == 'promoted' and visitor_language is not null and event == 'opportunity' and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by visitor_language, visitor_country";
        } else if (target.equalsIgnoreCase(DEVICESBYCOUNTRY)) {
            query = "select visitor_country,visitor_device,count(*) as count from events.last_3_months where visitor_country is not null and req_type == 'promoted' and visitor_device is not null and event == 'opportunity' and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by visitor_device, visitor_country";
        } else if (target.equalsIgnoreCase(FORMATSBYCOUNTRY)) {
            query = "select visitor_country,req_format,count(*) as count from events.last_3_months where visitor_country is not null and req_type == 'promoted' and req_format is not null and event == 'opportunity' and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by req_format, visitor_country";
        } else if (target.equalsIgnoreCase(CATEGORIESBYCOUNTRY)) {
            query = "select visitor_country,req_category,count(*) as count from events.last_3_months where visitor_country is not null and req_type == 'promoted' and req_category is not null and event == 'opportunity' and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by req_category, visitor_country";
        }


        JobReference jobId = null;
        try {
            jobId = startQuery(bigquery, PROJECT_ID, query);
        } catch (IOException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }

        // Poll for Query Results, return result output
        Job completedJob = null;
        try {
            completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);
        } catch (IOException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        } catch (InterruptedException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }

        // Return and display the results of the Query Job
        try {
            return getQueryResultsMap(bigquery, PROJECT_ID, completedJob);
        } catch (IOException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    /**
     * Creates an authorized BigQuery client service using the OAuth 2.0 protocol.
     *
     * @return an authorized BigQuery client
     * @throws IOException
     */
    public static Bigquery createAuthorizedClient() throws IOException, GeneralSecurityException {

        GoogleCredential credential = fromStream(BigQuery.class.getResourceAsStream(BQ_CREDENTIAL_FILENAME));
        credential = credential.createScoped(SCOPES);

        return new Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName(BQ_APP_NAME).build();
    }


    // [START start_query]

    /**
     * Creates a Query Job for a particular query on a dataset
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a String containing the project ID
     * @param querySql  the actual query string
     * @return a reference to the inserted query job
     * @throws IOException
     */
    public static JobReference startQuery(Bigquery bigquery, String projectId,
                                          String querySql) throws IOException {
        logger.info("\nInserting Query Job: {}\n", querySql);

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        config.setQuery(queryConfig);

        job.setConfiguration(config);
        queryConfig.setQuery(querySql);

        Insert insert = bigquery.jobs().insert(projectId, job);
        insert.setProjectId(projectId);
        JobReference jobId = insert.execute().getJobReference();

        logger.info("\nJob ID of Query Job is: {}\n", jobId.getJobId());

        return jobId;
    }

    /**
     * Polls the status of a BigQuery job, returns Job reference if "Done".
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a string containing the current project ID
     * @param jobId     a reference to an inserted query Job
     * @return a reference to the completed Job
     * @throws IOException
     * @throws InterruptedException
     */
    private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
            throws IOException, InterruptedException {
        // Variables to keep track of total query time
        long startTime = currentTimeMillis();
        long elapsedTime;

        while (true) {
            Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
            elapsedTime = currentTimeMillis() - startTime;
            logger.info("Job status ({}ms) {}: {}", elapsedTime,
                    jobId.getJobId(), pollJob.getStatus().getState());
            if (pollJob.getStatus().getState().equals(BQ_DONE_STATE)) {
                return pollJob;
            }
            // Pause execution for one second before polling job status again, to
            // reduce unnecessary calls to the BigQUery API and lower overall
            // application bandwidth.
            sleep(BQ_PAUSE_TIME);
        }
    }

    /**
     * Makes an API call to the BigQuery API.
     *
     * @param bigquery     an authorized BigQuery client
     * @param projectId    a string containing the current project ID
     * @param completedJob to the completed Job
     * @throws IOException
     * @returns Table (map f map of query results
     */
    private static Table<String, String, Long> getQueryResultsMap(Bigquery bigquery,
                                                                  String projectId, Job completedJob) throws IOException {
        GetQueryResultsResponse queryResult = bigquery.jobs()
                .getQueryResults(
                        projectId, completedJob
                                .getJobReference()
                                .getJobId()
                ).execute();
        List<TableRow> rows = queryResult.getRows();
        logger.info("\nQuery Results:\n------------\n");
        Table<String, String, Long> res = create();

        long total = 0;
        for (TableRow row : rows) {
            int i = 0;
            String country = null;
            String target = null;
            Long count = 0l;
            for (TableCell field : row.getF()) {
                if (i == 0) {
                    country = lowerCase((String) (field.getV()));
                    i++;
                } else if (i == 1) {
                    target = (String) field.getV();
                    i++;
                } else if (i == 2) {
                    total = total + valueOf((String) field.getV());
                    count = valueOf((String) field.getV());
                    i = 0;
                }
            }
            //logger.info(country + " : " + target + " : " + count);
            res.put(country, target, count);
        }

        res.put(TOTAL, TOTAL, total);
        // set column key totals
        for (String column : res.columnKeySet()) {
            Long tc = 0l;
            for (Long v : res.column(column).values()) {
                tc = v + tc;
            }
            res.put(TOTAL, column, tc);
        }
        // set country totals
        for (String rowKey : res.rowKeySet()) {
            Long tc = 0l;
            for (Long v : res.row(rowKey).values()) {
                tc = v + tc;
            }
            res.put(lowerCase(rowKey), TOTAL, tc);
        }
        return res;
    }

    public static Table<String, String, Long> getCountryCountTableFromFile(String target) throws ForecastException {
        return getTable(target);
    }
}