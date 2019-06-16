package client

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.FetchValue
import com.basho.riak.client.core.RiakCluster
import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.operations.SearchOperation
import com.basho.riak.client.core.util.BinaryValue

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

        val importer = CsvImporter(client)
//        importer.importProductCsv()
//        importer.importEenheidCsv()
//        importer.importAankoopCsv()
//        importer.importFiliaalCsv()

//        importer.importKlantCsv()

//        val productIndex = YokozunaIndex("klantIndex")
//        val storeIndex = StoreIndex.Builder(productIndex).build()
//        client.execute(storeIndex)

//        client.shutdown()

//        val animalsBucket = Namespace("animals-type", "animals")
//
//        val liono = RiakObject()
//                .setContentType("application/json")
//                .setValue(BinaryValue.create("{\"name_s\":\"Lion-o\",\"age_i\":30,\"leader_b\":true}"))
//
//        val lionoLoc = Location(animalsBucket, "liono")
//
//
//        val lionoStore = StoreValue.Builder(liono).withLocation(lionoLoc).build()
//        // The other StoreValue operations can be built the same way
//
//        client.execute(lionoStore)
//
        val searchOp = SearchOperation.Builder(BinaryValue.create("klantIndex"), "klantId_i:1")
                .build()
        cluster.execute(searchOp)
// This will display the actual results as a List of Maps:
        val results = searchOp.get().allResults
// This will display the number of results:
        println(results)


    }

    private fun fetchData(client: RiakClient, bucket: String, key: String) {
        val location = Location(Namespace(bucket), key)
        val fv = FetchValue.Builder(location).build()

        val response = client.execute(fv)
        val value = response.getValue(String::class.java)
        println(value)
    }

    private fun search(cluster: RiakCluster) {
        val searchOp = SearchOperation.Builder(BinaryValue.create("productIndex"), "productId:1")
                .build()
        cluster.execute(searchOp)
        // This will display the actual results as a List of Maps:
        val results = searchOp.get().allResults
        // This will display the number of results:
        println(results)

    }
}