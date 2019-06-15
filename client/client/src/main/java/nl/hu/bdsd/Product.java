package nl.hu.bdsd;

import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Product {
	public int productId_i;
	public String productOmschrijving_s;
	public String inhoudAantal_s;
	public int eenheidId_i;
	
	public void storeProduct(Product p, RiakClient client) throws ExecutionException, InterruptedException {
		Location productObjectLocation = new Location(new Namespace("producten","producten"), String.valueOf(p.productId_i));
		RiakObject obj = new RiakObject()
				.setContentType("application/json")
				.setValue(BinaryValue.create("{\"productId_i\":"+
								p.productId_i+",\"productOmschrijving_s\":\""+
								p.productOmschrijving_s+"\",\"inhoudAantal_s\":\""+
								p.inhoudAantal_s+"\",\"eenheidId_i\":"+p.eenheidId_i));
//		RegisterUpdate ru1 = new RegisterUpdate(Integer.toString(p.productId_i));
//		RegisterUpdate ru2 = new RegisterUpdate(p.productOmschrijving_s);
//		RegisterUpdate ru3 = new RegisterUpdate(p.inhoudAantal_s);
//		RegisterUpdate ru4 = new RegisterUpdate(Integer.toString(p.eenheidId_i));
//		MapUpdate mu = new MapUpdate()
//				.update("productId_i",ru1)
//				.update("productOmschrijving_s",ru2)
//				.update("inhoudAantal_s",ru3)
//				.update("eenheidId_i",ru4);
//		UpdateMap updateMap = new UpdateMap.Builder(productObjectLocation, mu).build();
		StoreValue store = new StoreValue.Builder(obj).withLocation(productObjectLocation).build();
		client.executeAsync(store);
//		client.executeAsync(updateMap);
		
		Location productIdSet = new Location(new Namespace("sets","product_info_sets"),"productIds");
		SetUpdate su = new SetUpdate().add(String.valueOf(p.productId_i));
		UpdateSet updateSet = new UpdateSet.Builder(productIdSet, su).build();
		client.executeAsync(updateSet);
	}
		
	public String toString() {
		return String.format("Product %d: %s. Aantal: %s, Eenheid: %d", productId_i, productOmschrijving_s, inhoudAantal_s, eenheidId_i);
	}
}
