package nl.hu.bdsd;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.HostAndPort;

import java.io.*;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class Client
{
	private static HostAndPort hp = HostAndPort.fromParts("52.226.68.195", 8087);
	private static RiakCluster setUpCluster() {
		RiakNode node = new RiakNode.Builder()
				.withMinConnections(10)
				.withRemoteAddress(hp)
				.build();
		
		RiakCluster cluster = new RiakCluster.Builder(node).build();
		cluster.start();
		return cluster;
	}
	private static int aankoopId = 1;
	
    public static void main( String[] args )
    {    	
    	String productCsv = "resources/product.csv";
    	String aankoopCsv = "resources/aankoop.csv";
    	String eenheidCsv = "resources/eenheid.csv";
    	String filiaalCsv = "resources/filiaal.csv";
    	String klantCsv = "resources/klant.csv";

        String separator = ",";
		Product newProduct = new Product();
		Aankoop newAankoop = new Aankoop();
		List<UpdateSet> klantLijst = new ArrayList<>();
        
        try {
        	RiakCluster cluster = setUpCluster();
        	RiakClient client = new RiakClient(cluster);

			System.out.println("connected");
        	BufferedReader reader = new BufferedReader(new FileReader(klantCsv));
			reader.lines().parallel().forEach((klantLine) -> {
				String[] klant = klantLine.split(separator);
				Location klantIdSet = new Location(new Namespace("sets","klant_info_sets"),"klantIds");
				SetUpdate su = new SetUpdate().add(cleanUp(klant[0]));
				UpdateSet updateSet = new UpdateSet.Builder(klantIdSet, su).build();
				client.executeAsync(updateSet);

				System.out.println("client su executed");
			});

        	reader.close();
			System.out.println("Clients added");
        	reader = new BufferedReader(new FileReader(productCsv));
//    		buildIndeces(client);
//        	Namespace productBucket = new Namespace("maps","producten");
//        	Namespace aankoopBucket = new Namespace("maps","aankopen");
        	reader.lines().parallel().forEach((prodLine) -> {
				String[] product = prodLine.split(separator);
				newProduct.productId = Integer.parseInt(cleanUp(product[0]));
				newProduct.productOmschrijving = cleanUp(product[1]);
				newProduct.inhoudAantal = cleanUp(product[2]);
				newProduct.eenheidId = Integer.parseInt(cleanUp(product[3]));
				try {
					newProduct.storeProduct(newProduct, client);
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println(newProduct);
			});

        	reader.close();
        	System.out.println("Products added");
//        	Set<Product> products = fetchAllProducten(client);
//        	for (Product p : products) { 
//        		System.out.println(p);
//        	}
        	
//        	klantParen(cluster, client);
//        	fetchAllAankopen(client,cluster).forEach(aankoop -> {
//
//        		System.out.println(aankoop);
//
//        	});
        	reader = new BufferedReader(new FileReader(aankoopCsv));

        	reader.lines().parallel().forEach((aankoopLine) -> {
				String[] aankoop = aankoopLine.split(separator);
				newAankoop.aankoopId = getAndIncrement();
				newAankoop.klantId = Integer.parseInt(cleanUp(aankoop[0]));
				newAankoop.filiaalId = Integer.parseInt(cleanUp(aankoop[1]));
				newAankoop.productId = Integer.parseInt(cleanUp(aankoop[2]));
				try {
					newAankoop.datum = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(cleanUp(aankoop[3]));
				} catch (ParseException e ) {
					e.printStackTrace();
				}
				newAankoop.aantal = Integer.parseInt(cleanUp(aankoop[4]));
				System.out.println(newAankoop);
				try {
					newAankoop.storeAankoop(newAankoop,client);
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			});

        	reader.close();
        	cluster.shutdown();
        } catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			System.out.println("done");
		}
        
    }
    
	private static String cleanUp(String s) {
		s = s.substring(1,s.length()-1);
		return s;
	}
	
	public static Set<Product> fetchAllProducten(RiakClient client, RiakCluster cluster) throws ExecutionException, InterruptedException, UnresolvedConflictException {
		String index = "aankoopIndex";
		Set<String> productIdSet = new HashSet<String>();
		Set<Product> productSet = new HashSet<Product>();
	    Location productIdSetLoc = new Location(new Namespace("sets", "product_info_sets"), "productIds");
	    FetchSet fetchProductIdSet = new FetchSet.Builder(productIdSetLoc).build();
	    RiakSet set = client.execute(fetchProductIdSet).getDatatype();
	    set.view().forEach((BinaryValue productId)-> { productIdSet.add(productId.toString()); });

	    productIdSet.forEach(productId->{
	    	Location productLocation = new Location(new Namespace("maps","producten"),productId);
	    	FetchValue fetch = new FetchValue.Builder(productLocation).build();
	    	Product product = null;
			try {
				product = client.execute(fetch).getValue(Product.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	productSet.add(product);
	    });
	    
	    return productSet;
	}
	
	public static Set<Aankoop> fetchAllAankopen(RiakClient client, RiakCluster cluster) throws ExecutionException, InterruptedException, UnresolvedConflictException {
		String index = "aankoopIndex";
		Set<String> aankoopIdSet = new HashSet<String>();
		Set<Aankoop> aankoopSet = new HashSet<Aankoop>();
	    Location aankoopIdSetLoc = new Location(new Namespace("sets", "aankoop_info_sets"), "aankoopIds");
	    FetchSet fetchAankoopIdSet = new FetchSet.Builder(aankoopIdSetLoc).build();
	    RiakSet set = client.execute(fetchAankoopIdSet).getDatatype();
	    set.view().forEach(aankoopId->{ aankoopIdSet.add(aankoopId.toString()); });
	    aankoopIdSet.forEach(id->{
	    	Location aankoopLocation = new Location(new Namespace("maps","aankopen"),id);
	    	FetchValue fetch = new FetchValue.Builder(aankoopLocation).build();
	    	String query = "aankoopId_register:"+id;
	    	SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(index), query).build();
	    	cluster.execute(searchOp);
	    	Aankoop aankoop = null;
			try {
				SearchOperation.Response results = searchOp.get();
				results.getAllResults().stream().forEach(result -> {
					if (Integer.parseInt(cleanUp(result.get("klantId_register").toString())) > 59) {
						System.out.println(result.toString());
					}
				});
				aankoop = client.execute(fetch).getValue(Aankoop.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	aankoopSet.add(aankoop);
	    });
	    
	    return aankoopSet;
	}
	
	//Geeft klantparen die minstens 4 van hetzelfde product hebben gekocht. 
	private static void klantParen(RiakCluster cluster, RiakClient client) throws InterruptedException, ExecutionException {
		Map<String,List<String>> klantProducten = new HashMap<String,List<String>>();
		
		String index = "aankoopIndex";
		Location klantIdLoc = new Location(new Namespace("sets","klant_info_sets"),"klantIds");
		
		FetchSet fetch = new FetchSet.Builder(klantIdLoc).build();
		FetchSet.Response response = client.execute(fetch);
		RiakSet rSet = response.getDatatype();
		
		Set<BinaryValue> klantIds = rSet.view();
		
		klantIds.stream().forEach(klantId -> {
			Set<String> productIdSet = new TreeSet<String>();
			List<String> productIdList = new ArrayList<String>();
			String query = "klantId_register:"+klantId.toString();
			SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(index), query).build();
			cluster.execute(searchOp);
			try {
				SearchOperation.Response results = searchOp.get();
				for (Map<String,List<String>> val : results.getAllResults()) {
					String s = cleanUp(val.get("productId_register").toString());
					if (!productIdList.contains(s)) {
						productIdList.add(s);						
					}
				}
				System.out.println(productIdList);
				results.getAllResults().stream().forEach(map -> {
//					System.out.println(map);
					String s = cleanUp(map.get("productId_register").toString());
					productIdSet.add(s);
				});
				System.out.println("Klant: " + klantId + " Producten: " + productIdList);
				if (!productIdSet.isEmpty()) klantProducten.put(klantId.toString(),productIdList);
		
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		System.out.println(klantProducten);
		
//		Map<String,Set<String[]>> prods = new HashMap<String,Set<String[]>>();
//		klantProducten.forEach((klant,producten) -> {
//			int klantId = Integer.parseInt(klant);
//			Set<String> nextKlant = klantProducten.get(Integer.toString(klantId < 59 ? klantId+1 : klantId));
//			int counter = 0;
//			for (String p : producten) {
//				if (nextKlant.contains(p)) {
//					counter++;
//					System.out.println(klantId + " hit Counter @ " + counter);
//				}
//				if (counter >= 4) {
//					System.out.println("Got one!");
//				}
//			}
//		});
//		String regex = "[0-5]?[0-9]";
//		klantAankopen.forEach(klantProd -> {
//			klantProd.keySet().forEach(e -> {
//				if (!e.matches(regex)) {
//					klantAankopen.remove(klantProd.get(e));
//				}
//			});
//		});
		
		
//		String query = "aantal_counter:[* TO *]";
//		SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(index), query).build();
//		cluster.execute(searchOp);
//		SearchOperation.Response results = searchOp.get();
//		System.out.println(results.numResults());
//		List<Map<String,List<String>>> aankopen = results.getAllResults();
////		List<Map<String,Set<String>>>
//		aankopen.forEach(map -> {
//			String klantId = map.get("klantId_register").toString();
//			System.out.println(klantId);
//			
//		});
	}
	
	private void productParen(int productId1, int productId2) {
	
	}
	
	private static void buildIndeces(RiakClient client) throws ExecutionException, InterruptedException {
		YokozunaIndex aankoopIndex = new YokozunaIndex("aankoopIndex", "_yz_default");
		StoreIndex storeIndex1 = new StoreIndex.Builder(aankoopIndex)
		        .build();
		client.execute(storeIndex1);
		
		YokozunaIndex productIndex = new YokozunaIndex("productIndex", "_yz_default");
		StoreIndex storeIndex2 = new StoreIndex.Builder(productIndex)
		        .build();
		client.execute(storeIndex2);
	}

	private static int getAndIncrement() {
		aankoopId++;
		return aankoopId -1;
	}
}
