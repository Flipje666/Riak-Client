package client

import com.basho.riak.client.api.RiakClient
import com.opencsv.CSVReader

import java.io.BufferedReader
import java.io.FileReader
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.api.commands.kv.StoreValue

class Product(val productId_i: Int, val productOmschrijving_s: String, val inhoudAantal_s: String, val eenheidId_i: Int)
class Eenheid(val eenheidId_i: Int, val eenheidOmschrijving_s: String)
class Aankoop(val aankoopId_i: Int, val klantId_i: Int, val filiaalId_i: Int, val productId_i: Int, val datum_s: String, var aantal_i: Int)
class Klant(val klantId_i: Int)
class Filiaal(val filiaalId_i: Int)

internal class CsvImporter(private val client: RiakClient) {
    private var productCsv = "resources/product.csv"
    private var aankoopCsv = "resources/aankoop.csv"
    private var eenheidCsv = "resources/eenheid.csv"
    private var filiaalCsv = "resources/filiaal.csv"
    private var klantCsv = "resources/klant.csv"

    private fun storeData(riakObject: Any, key: String, bucket: String, buckettype: String) {
        val location = Location(Namespace(buckettype, bucket), key)
        val sv = StoreValue.Builder(riakObject).withLocation(location).
                build()

        client.executeAsync(sv)
    }

    fun importProductCsv() {
        val fileReader = BufferedReader(FileReader(productCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val producten = mutableListOf<Product>()

        for (record in csv) {
            producten.add(Product(record[0].toInt(), record[1], record[2], record[3].toInt()))
        }

        for (product in producten) {
            storeData(product, product.productId_i.toString(), "product2", "product-type")
        }
    }

    fun importEenheidCsv() {
        val fileReader = BufferedReader(FileReader(eenheidCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val eenheden = mutableListOf<Eenheid>()

        for (record in csv) {
            eenheden.add(Eenheid(record[0].toInt(), record[1]))
        }

        for (eenheid in eenheden) {
            storeData(eenheid, eenheid.eenheidId_i.toString(), "eenheid2", "eenheid-type")
        }

    }

    fun importAankoopCsv() {
        val fileReader = BufferedReader(FileReader(aankoopCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val aankopen = mutableListOf<Aankoop>()

        var aankoopId = 1
        for (record in csv.drop(1)) {
            aankopen.add(Aankoop(aankoopId, record[0].toInt(), record[1].toInt(), record[2].toInt(), record[3], record[4].toInt()))
            aankoopId++
        }

        for (aankoop in aankopen) {
            storeData(aankoop, aankoop.aankoopId_i.toString(), "aankoop2", "aankoop-type")
        }

    }

    fun importFiliaalCsv() {
        val fileReader = BufferedReader(FileReader(filiaalCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val filialen = mutableListOf<Filiaal>()

        for (record in csv) {
            filialen.add(Filiaal(record[0].toInt()))
        }

        for (filiaal in filialen) {
            storeData(filiaal, filiaal.filiaalId_i.toString(), "filiaal2", "filiaal-type")
        }

    }

    fun importKlantCsv() {
        val fileReader = BufferedReader(FileReader(klantCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val klanten = mutableListOf<Klant>()

        for (record in csv) {
            klanten.add(Klant(record[0].toInt()))
        }

        for (klant in klanten) {
            storeData(klant, klant.klantId_i.toString(), "klant5", "klant-type2")
        }

    }

}