import com.google.api.gax.paging.Page
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.BigQuery.DatasetListOption
import com.google.cloud.bigquery.BigQuery.TableListOption
import java.io.File

import java.util.function.Consumer


class BqTests(private val projectId: String, serviceAccountPath: String) {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    private val bigQuery = BqAuth.fromFile(projectId, File(serviceAccountPath))

    fun createRandomJobId(): JobId = JobId.newBuilder()
        .setRandomJob()  // let BQ cloud client generated random JobId
        .setLocation("US") // US and EU does not have to be specified
        .build()

    fun query(query: String, jobId: JobId, fetchDelayMs: Long = 0) {
        try {
            val queryConfig = QueryJobConfiguration.newBuilder(query).build()
            SimpleLog.log("Running query with JobId: ${jobId.job}")
            val results = bigQuery.query(queryConfig, jobId)
            SimpleLog.log("Query ${jobId.job} results:")
            results
                .iterateAll()
                .forEach(Consumer { row: FieldValueList ->
                    SimpleLog.log("Query columns: ${row.size}")
                    row.forEach(Consumer {
                        print("$it,")
                    })
                    println("")
                    if (fetchDelayMs > 0) {
                        Thread.sleep(fetchDelayMs)
                    }
                })
            SimpleLog.log("Query performed successfully.")
        } catch (e: BigQueryException) {
            SimpleLog.log("Query not performed \n$e")
        } catch (e: InterruptedException) {
            SimpleLog.log("Query interrupted \n$e")
        }
    }

    fun cancelQuery(jobId: JobId) {
        try {
            bigQuery.cancel(jobId)
        } catch (e: BigQueryException) {
            SimpleLog.log("Query cancel failed \n$e")
        }
    }

    fun listDatasets(projId: String = "") {
        // https://cloud.google.com/bigquery/docs/listing-datasets
        val useProjId = if (projId === "") projectId else projId
        try {
            // can be filtered by dataset label
            val datasets: Page<Dataset>? = bigQuery.listDatasets(useProjId, DatasetListOption.pageSize(100))
            if (datasets == null) {
                SimpleLog.log("Project $useProjId does not contain any datasets")
                return
            }
            // !!!method returns partial information on each dataset:
            //   (Dataset#getDatasetId(), Dataset#getFriendlyName() and Dataset#getGeneratedId()).
            datasets
                .iterateAll()
                .forEach { dataset ->
                    SimpleLog.log("DatasetName:${dataset.datasetId.dataset}, FriendlyName:${dataset.friendlyName}")
                }
        } catch (e: BigQueryException) {
            // ? Seems more like ProjectId does not exists
            SimpleLog.log("Project $useProjId does not contain any datasets \n$e")
        }
    }

    fun listTables(datasetName: String) {
        // https://cloud.google.com/bigquery/docs/samples/bigquery-list-tables
        try {
            val datasetId = DatasetId.of(projectId, datasetName)
            val tables = bigQuery.listTables(datasetId, TableListOption.pageSize(100))
            tables.iterateAll().forEach(Consumer { table: Table ->
                SimpleLog.log("TableName: ${table.tableId.table}")
            })
            SimpleLog.log("Tables listed successfully.")
        } catch (e: BigQueryException) {
            SimpleLog.log("Tables were not listed. Error occurred: $e")
        }
    }

    fun listTableColumns(datasetName: String, tableName: String) {
        SimpleLog.log("List table columns - start")
        try {
            val tableId = TableId.of(projectId, datasetName, tableName)
            SimpleLog.log("getTables - start")
            val table: Table = bigQuery.getTable(tableId)
            SimpleLog.log("getTables - finish")
            val def = table.getDefinition<TableDefinition>()
            if (def.schema != null) {
                def.schema!!.fields.forEach {
                    SimpleLog.log("Filed: ${it.name} = ${it.type}")
                }
            } else {
                SimpleLog.log("Cannot retrieve table schema")
            }
        } catch (e: BigQueryException) {
            SimpleLog.log("Failed to retrieve table schema. Error: $e")
        }
        SimpleLog.log("List table columns - finish")
    }
}
