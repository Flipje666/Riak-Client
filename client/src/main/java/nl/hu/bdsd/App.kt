package nl.hu.bdsd

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.FetchValue
import com.basho.riak.client.core.RiakCluster
import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.HostAndPort

object App {
    private val hp = HostAndPort.fromParts("40.87.43.33", 8087)

    private fun setUpCluster(): RiakCluster {
        val node = RiakNode.Builder()
                .withMinConnections(10)
                .withRemoteAddress(hp)
                .build()

        val cluster = RiakCluster.Builder(node).build()
        cluster.start()
        return cluster
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val cluster = setUpCluster()
        val client = RiakClient(cluster)
//        val importer = CsvImporter(client)

//        importer.importProductCsv()
//        importer.importEenheidCsv()
//        importer.importAankoopCsv()
//        importer.importFiliaalCsv()
//        importer.importKlantCsv()

        fetchData(client, "producten", "1")
        fetchData(client, "eenheden", "1")
        fetchData(client, "klanten", "1")
        fetchData(client, "filialen", "1")
        fetchData(client, "aankopen", "1")

        client.shutdown()
    }

    private fun fetchData(client : RiakClient, bucket: String, key: String) {
        val location = Location(Namespace(bucket), key)
        val fv = FetchValue.Builder(location).build()

        val response = client.execute(fv)
        val value = response.getValue(String::class.java)
        println(value)
    }
}