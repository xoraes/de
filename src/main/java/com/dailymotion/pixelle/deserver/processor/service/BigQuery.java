package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;


public class BigQuery {
    private static final String PROJECT_ID = "dailymotion-pixelle";
    // Static variables for API scope, callback URI, and HTTP/JSON functions
    private static final List<String> SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
    private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
    /**
     * Global instances of HTTP transport and JSON factory objects.
     */
    private static final HttpTransport TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static Logger logger = LoggerFactory.getLogger(BigQuery.class);

    public static Table<String, String, Long> getCountryCountTable(String target) throws IOException, InterruptedException, GeneralSecurityException {
        Bigquery bigquery = createAuthorizedClient();
        String query = null;

        if (target.equalsIgnoreCase(DeHelper.EVENTSBYCOUNTRY)) {
            query = "select visitor_country, event, count(*) as count from events.last_3_months where event is not null and (event == 'opportunity' or event == 'view') and visitor_country is not null and (occurrence is null or occurrence == 1) and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by event, visitor_country";
        } else if (target.equalsIgnoreCase(DeHelper.LANGUAGEBYCOUNTRY)) {
            query = "select visitor_country,visitor_language,count(*) as count from events.last_3_months where visitor_country is not null and visitor_language is not null and event == 'view' and occurrence == 1 and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by visitor_language, visitor_country";
        } else if (target.equalsIgnoreCase(DeHelper.DEVICESBYCOUNTRY)) {
            query = "select visitor_country,visitor_device,count(*) as count from events.last_3_months where visitor_country is not null and visitor_device is not null and event == 'view' and occurrence == 1 and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by visitor_device, visitor_country";
        } else if (target.equalsIgnoreCase(DeHelper.FORMATSBYCOUNTRY)) {
            query = "select visitor_country,req_format,count(*) as count from events.last_3_months where visitor_country is not null and req_format is not null and event == 'view' and occurrence == 1 and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by req_format, visitor_country";
        } else if (target.equalsIgnoreCase(DeHelper.CATEGORIESBYCOUNTRY)) {
            query = "select visitor_country,req_category,count(*) as count from events.last_3_months where visitor_country is not null and req_category is not null and event == 'view' and occurrence == 1 and timestamp < timestamp(current_date()) and timestamp >= timestamp(date_add(timestamp(current_date()), -21, 'DAY')) group by req_category, visitor_country";
        }


        JobReference jobId = startQuery(bigquery, PROJECT_ID, query);

        // Poll for Query Results, return result output
        Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

        // Return and display the results of the Query Job
        return getQueryResultsMap(bigquery, PROJECT_ID, completedJob);
    }

    /**
     * Creates an authorized BigQuery client service using the OAuth 2.0 protocol
     * <p>
     * This method first creates a BigQuery authorization URL, then prompts the
     * user to visit this URL in a web browser to authorize access. The
     * application will wait for the user to paste the resulting authorization
     * code at the command line prompt.
     *
     * @return an authorized BigQuery client
     * @throws IOException
     */
    public static Bigquery createAuthorizedClient() throws IOException, GeneralSecurityException {

        GoogleCredential credential = GoogleCredential.fromStream(BigQuery.class.getResourceAsStream("/bq.json"));
        credential = credential.createScoped(SCOPES);

        return new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("pixelle-de").build();
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
        long startTime = System.currentTimeMillis();
        long elapsedTime;

        while (true) {
            Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
            elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("Job status ({}ms) {}: {}", elapsedTime,
                    jobId.getJobId(), pollJob.getStatus().getState());
            if (pollJob.getStatus().getState().equals("DONE")) {
                return pollJob;
            }
            // Pause execution for one second before polling job status again, to
            // reduce unnecessary calls to the BigQUery API and lower overall
            // application bandwidth.
            Thread.sleep(1000);
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
        Table<String, String, Long> res = HashBasedTable.create();

        long total = 0;
        for (TableRow row : rows) {
            int i = 0;
            String country = null;
            String target = null;
            Long count = 0l;
            for (TableCell field : row.getF()) {
                if (i == 0) {
                    country = (String) field.getV();
                    i++;
                } else if (i == 1) {
                    target = (String) field.getV();
                    i++;
                } else if (i == 2) {
                    total = total + Long.valueOf((String) field.getV());
                    count = Long.valueOf((String) field.getV());
                    i = 0;
                }
            }
            res.put(country, target, count);
        }
        res.put("total", "total", total);
        // set column key totals
        for (String column : res.columnKeySet()) {
            Long tc = 0l;
            for (Long v : res.column(column).values()) {
                tc = v + tc;
            }
            res.put("total", column, tc);
        }
        // set country totals
        for (String rowKey : res.rowKeySet()) {
            Long tc = 0l;
            for (Long v : res.row(rowKey).values()) {
                tc = v + tc;
            }
            res.put(rowKey, "total", tc);
        }
        return res;
    }

    public static Table<String, String, Long> getCountryCountTableFromFile(String target) {
        return FileLoader.getTable(target);
    }
}