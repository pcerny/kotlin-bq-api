import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import java.io.File
import java.io.FileInputStream
import java.io.IOException

/**
 * https://cloud.google.com/docs/authentication/best-practices-applications
 * OIDC integration skipped. Some customers have user account with access to all GC services including BQ.
 * Token would have to be forwarded by client.
 *
 * BigQuery instance does not have to be closed. It is a prescription how to contact BigQuery.
 *
 * https://cloud.google.com/bigquery/quotas#all_api_requests
 * Let's share BigQuery instance as much as possible - there can be limit to number of authentication requests
 * Encapsulate it in a way it can be made thread-safe, better said coroutine-safe + thread-safe
 */
object BqAuth {
    /**
     * Authenticate by explicitly specifying path to credentials file.
     * Is projectId optional???
     */
    @Throws(IOException::class)
    fun fromFile(projectId: String, filePath: File): BigQuery {
       val credentials =  FileInputStream(filePath).use { serviceAccountStream ->
            ServiceAccountCredentials.fromStream(serviceAccountStream)
       }

        // Instantiate a client.
        return BigQueryOptions.newBuilder()
            .setCredentials(credentials)
            .setProjectId(projectId)
            .build()
            .service
    }

    /**
     * Set ENV GOOGLE_APPLICATION_CREDENTIALS to path to service account credentials file.
     * BQ client does all for you. ProjectId does not have to be defined. -> ??some default
     * https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable
     */
    fun fromEnv(): BigQuery = BigQueryOptions.getDefaultInstance().service
}
