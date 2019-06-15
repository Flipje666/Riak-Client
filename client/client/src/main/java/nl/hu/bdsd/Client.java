package nl.hu.bdsd;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.SearchOperation;
import com.basho.riak.client.core.operations.YzDeleteIndexOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.HostAndPort;

import java.io.*;
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

public class Client
{
	private static HostAndPort hp = HostAndPort.fromParts("40.76.205.58", 8087);
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
        
        try {
        	RiakCluster cluster = setUpCluster();
        	RiakClient client = new RiakClient(cluster);

			System.out.println("connected");
//			buildIndeces(client);
//        	BufferedReader reader = new BufferedReader(new FileReader(klantCsv));
//			reader.lines().parallel().unordered().distinct().forEach((klantLine) -> {
//				String[] klant = klantLine.split(separator);
//				Location klantIdSet = new Location(new Namespace("sets","klant_info_sets"),"klantIds");
//				SetUpdate su = new SetUpdate().add(cleanUp(klant[0]));
//				UpdateSet updateSet = new UpdateSet.Builder(klantIdSet, su).build();
//				client.executeAsync(updateSet);
//			});
//
//        	reader.close();
//			System.out.println("Clients added");
//        	reader = new BufferedReader(new FileReader(productCsv));
//
//        	reader.lines().parallel().unordered().distinct().forEach((prodLine) -> {
//				String[] product = prodLine.split(separator);
//				newProduct.productId_i = Integer.parseInt(cleanUp(product[0]));
//				newProduct.productOmschrijving_s = cleanUp(product[1]);
//				newProduct.inhoudAantal_s = cleanUp(product[2]);
//				newProduct.eenheidId_i = Integer.parseInt(cleanUp(product[3]));
//				try {
//					newProduct.storeProduct(newProduct, client);
//					System.out.println(newProduct);
//				} catch (ExecutionException e) {
//					e.printStackTrace();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			});
//
//
//        	reader.close();
//        	System.out.println("Products added");
//
//        	reader = new BufferedReader(new FileReader(aankoopCsv));
//
//        	reader.lines().parallel().unordered().distinct().forEach((aankoopLine) -> {
//				String[] aankoop = aankoopLine.split(separator);
//				newAankoop.aankoopId_i = getAndIncrement();
//				newAankoop.klantId_i = Integer.parseInt(cleanUp(aankoop[0]));
//				newAankoop.filiaalId_i = Integer.parseInt(cleanUp(aankoop[1]));
//				newAankoop.productId_i = Integer.parseInt(cleanUp(aankoop[2]));
//				try {
//					newAankoop.datum_dt = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(cleanUp(aankoop[3]));
//				} catch (ParseException e ) {
//					e.printStackTrace();
//				}
//				newAankoop.aantal_i = Integer.parseInt(cleanUp(aankoop[4]));
//
//				try {
//					newAankoop.storeAankoop(newAankoop,client);
//					System.out.println(newAankoop);
//				} catch (ExecutionException e) {
//					e.printStackTrace();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			});
//
//        	reader.close();
//			System.out.println("Customers added");
			SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create("aankoopIndex"), "klantId_i:2500").build();
			cluster.execute(searchOp);
			List<Map<String, List<String>>> results = searchOp.get().getAllResults();
			System.out.println(results);
//			FetchValue fetch = new FetchValue.Builder(new Location(new Namespace("aankopen","aankopen"),String.valueOf(1)))
//					.build();
//			FetchValue.Response response = client.execute(fetch);
//			RiakObject obj = response.getValue(RiakObject.class);
//			System.out.println(obj.getValue());

//			klantParen(cluster, client);
//        	cluster.shutdown();
//        } catch (IOException e) {
//			e.printStackTrace();
		} catch (ExecutionException e) {
        	e.printStackTrace();
		} catch (InterruptedException e) {
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
		
		klantIds.stream().distinct().forEach(klantId -> {
			Set<String> productIdSet = new TreeSet<String>();
			List<String> productIdList = new ArrayList<String>();
			String query = "klantId_i:"+klantId.toString();
			SearchOperation searchOp = new SearchOperation.Builder(BinaryValue.create(index), query).build();
			cluster.execute(searchOp);
			try {
				SearchOperation.Response results = searchOp.get();
				for (Map<String,List<String>> val : results.getAllResults()) {
					String s = cleanUp(val.get("productId_i").toString());
					if (!productIdList.contains(s)) {
						productIdList.add(s);						
					}
				}
				System.out.println(productIdList);
				results.getAllResults().stream().forEach(map -> {
//					System.out.println(map);
					String s = cleanUp(map.get("productId_i").toString());
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
//			int klantId_i = Integer.parseInt(klant);
//			Set<String> nextKlant = klantProducten.get(Integer.toString(klantId_i < 59 ? klantId_i+1 : klantId_i));
//			int counter = 0;
//			for (String p : producten) {
//				if (nextKlant.contains(p)) {
//					counter++;
//					System.out.println(klantId_i + " hit Counter @ " + counter);
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
//			String klantId_i = map.get("klantId_register").toString();
//			System.out.println(klantId_i);
//			
//		});
	}
	
	private void productParen(int productId1, int productId2) {
	
	}
	
	private static void buildIndeces(RiakClient client) throws ExecutionException, InterruptedException {
		YokozunaIndex aankoopIndex = new YokozunaIndex("aankoopIndex", "_yz_default").withNVal(1);
		StoreIndex storeIndex1 = new StoreIndex.Builder(aankoopIndex).build();
		client.execute(storeIndex1);

		YokozunaIndex productIndex = new YokozunaIndex("productIndex", "_yz_default").withNVal(1);
		StoreIndex storeIndex2 = new StoreIndex.Builder(productIndex).build();
		client.execute(storeIndex2);

//		Namespace aankopen = new Namespace("aankopen","aankopen");
//		Namespace producten = new Namespace("producten","producten");
//		StoreBucketProperties storeAankoopPropsOp = new StoreBucketProperties.Builder(aankopen).withSearchIndex("aankoopIndex").build();
//		StoreBucketProperties storeProductPropsOp = new StoreBucketProperties.Builder(producten).withSearchIndex("productIndex").build();
//		client.execute(storeAankoopPropsOp);
//		client.execute(storeProductPropsOp);
	}

	private static int getAndIncrement() {
		aankoopId++;
		return aankoopId -1;
	}
}
