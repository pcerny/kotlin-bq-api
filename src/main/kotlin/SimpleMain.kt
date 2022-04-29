import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

object SimpleMain {
    private val longQuery = """
            select c.*, b.*, a.*,
                   COUNT(DISTINCT a.lv_pk) over () as distinct_lv_pk
            from (
              select x1.*, x2.l_pk as x2_pk,
                     LAST_VALUE(x1.l_pk) OVER (partition by x1.l_shipmode ORDER BY x1.l_shipdate desc) as lv_pk
              from tiger_tpch_5c0f4d06a19d01f0.out_tpch_vw__lineitem x1
              left join tiger_tpch_5c0f4d06a19d01f0.out_tpch_vw__lineitem x2 on (1=1)
            ) a
            join tiger_tpch_5c0f4d06a19d01f0.out_tpch_tab__orders b on (b.o_orderkey = a.l_orderkey)
            left join tiger_tpch_5c0f4d06a19d01f0.out_tpch_tab__customer c on (b.o_custkey = c.c_custkey)
            limit 200
        """.trimIndent()

    private val queryCampaigns = """
            SELECT gr__campaign_channel_id, category, `type`, budget, spend, r__campaigns__campaign_id
              FROM tiger_pce_demo.campaign_channels
             LIMIT 20;""".trimIndent()

    private val queryLineItems = """
            SELECT *
              FROM tiger_pce_demo.order_lines
             LIMIT 200;""".trimIndent()

    @JvmStatic
    fun main(args: Array<String>) {
        val bqT = BqTests(
            System.getenv("PROJECT_ID") ?: "projectId",
            System.getenv("SERVICE_ACCOUNT_CREDENTIALS") ?: "creds/creds.json"
        )
        bqT.query(queryCampaigns, bqT.createRandomJobId())
//        bqT.listDatasets("non-existing-project")
//        bqT.listDatasets()
        bqT.listTables("tiger_demo")
        bqT.listTableColumns("tiger_demo", "customers")
        testCancel(bqT)
//        testThreadSafetyLongNaive(bqT)
//        testThreadSafetyShortNaive(bqT)
    }

    private fun testCancel(bqT: BqTests) = runBlocking {
        SimpleLog.log("test cancel - start")
        val jobId = bqT.createRandomJobId()
        val handle = async(Dispatchers.IO) {
            SimpleLog.log("Starting async...")
            bqT.query(longQuery, jobId)
        }
        SimpleLog.log("Waiting 20 sec")
        delay(20000)
        SimpleLog.log("Cancelling query")
        bqT.cancelQuery(jobId)

        SimpleLog.log("Waiting query to finish")
        try {
            handle.await()
        } catch (e: Exception) {
            SimpleLog.log("Query await failed - $e")
        }
        SimpleLog.log("test cancel - finished")
    }

    private fun testThreadSafetyLongNaive(bqT: BqTests) = runBlocking {
        SimpleLog.log("test ThreadSafetyLong - start")
        val jobId = bqT.createRandomJobId()
        val handleLong = async(Dispatchers.IO) {
            SimpleLog.log("Starting async with long query...")
            bqT.query(longQuery, jobId)
        }
        SimpleLog.log("Waiting 2 sec")
        delay(2000)
        SimpleLog.log("Executing other queries by the same instance")
        repeat(5) {
            bqT.query("SELECT $it as num", bqT.createRandomJobId())
        }
        SimpleLog.log("Waiting 2 sec")
        delay(2000)
        SimpleLog.log("Cancelling long query")
        bqT.cancelQuery(jobId)

        SimpleLog.log("Waiting query to finish")
        try {
            handleLong.await()
        } catch (e: Exception) {
            SimpleLog.log("Query await failed - $e")
        }
        SimpleLog.log("test ThreadSafetyLong - finished")
    }

    private fun testThreadSafetyShortNaive(bqT: BqTests) = runBlocking {
        SimpleLog.log("test ThreadSafetyShort - start")
        val jobId = bqT.createRandomJobId()
        val handleLong = async(Dispatchers.IO) {
            SimpleLog.log("Starting async with short query a and lots of results...")
            bqT.query(queryLineItems, jobId, 10)
        }
        val handleShort = async(Dispatchers.IO) {
            SimpleLog.log("Starting async with burst select 1...")
            repeat(10) {
                bqT.query("SELECT $it as num", bqT.createRandomJobId())
            }
        }
        SimpleLog.log("Waiting ...")
        try {
            handleLong.await()
            handleShort.await()
        } catch (e: Exception) {
            SimpleLog.log("Query await failed - $e")
        }
        SimpleLog.log("test ThreadSafetyShort - finished")
    }
}
