package nl.hu.bdsd

import com.basho.riak.client.api.RiakClient
import com.opencsv.CSVReader

import java.io.BufferedReader
import java.io.FileReader
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.api.commands.kv.StoreValue

class Product(val productId: Int, val productOmschrijving: String, val inhoudAantal: Float, val eenheidId: Int)
class Eenheid(val eenheidId: Int, val eenheidOmschrijving: String)
class Aankoop(val aankoopId: Int, val klantId: Int, val filiaalId: Int, val productId: Int, val datum: String, var aantal: Int)
class Klant(val klantId: Int)
class Filiaal(val filiaalId: Int)

internal class CsvImporter(private val client: RiakClient) {
    private var productCsv = "client/src/main/resources/product.csv"
    private var aankoopCsv = "client/src/main/resources/aankoop.csv"
    private var eenheidCsv = "client/src/main/resources/eenheid.csv"
    private var filiaalCsv = "client/src/main/resources/filiaal.csv"
    private var klantCsv = "client/src/main/resources/klant.csv"

    private fun storeData(riakObject: Any, key: String, bucket: String) {
        val location = Location(Namespace(bucket), key)
        val sv = StoreValue.Builder(riakObject).withLocation(location).build()

        client.executeAsync(sv)
    }

    fun importProductCsv() {
        val fileReader = BufferedReader(FileReader(productCsv))
        val csvReader = CSVReader(fileReader)
        val csv = csvReader.readAll()

        val producten = mutableListOf<Product>()

        for (record in csv) {
            producten.add(Product(record[0].toInt(), record[1], record[2].toFloat(), record[3].toInt()))
        }

        for (product in producten) {
            storeData(product, product.productId.toString(), "producten")
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
            storeData(eenheid, eenheid.eenheidId.toString(), "eenheden")
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
            storeData(aankoop, aankoop.aankoopId.toString(), "aankopen")
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
            storeData(filiaal, filiaal.filiaalId.toString(), "filialen")
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
            storeData(klant, klant.klantId.toString(), "klanten")
        }

    }

}