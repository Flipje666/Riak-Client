package nl.hu.bdsd;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.MapUpdate;
import com.basho.riak.client.api.commands.datatypes.RegisterUpdate;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateMap;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

public class Product {
	public int productId;
	public String productOmschrijving;
	public String inhoudAantal;
	public int eenheidId;
	
	public void storeProduct(Product p, RiakClient client) throws ExecutionException, InterruptedException {
		Location productObjectLocation = new Location(new Namespace("maps","producten"), String.valueOf(p.productId));
		RegisterUpdate ru1 = new RegisterUpdate(Integer.toString(p.productId));
		RegisterUpdate ru2 = new RegisterUpdate(p.productOmschrijving);
		RegisterUpdate ru3 = new RegisterUpdate(p.inhoudAantal);
		RegisterUpdate ru4 = new RegisterUpdate(Integer.toString(p.eenheidId));
		MapUpdate mu = new MapUpdate()
				.update("productId",ru1)
				.update("productOmschrijving",ru2)
				.update("inhoudAantal",ru3)
				.update("eenheidId",ru4);
		UpdateMap updateMap = new UpdateMap.Builder(productObjectLocation, mu).build();
		StoreValue store = new StoreValue.Builder(p).withLocation(productObjectLocation).build();
		client.executeAsync(store);
		client.executeAsync(updateMap);
		System.out.println("productmap updated");
		
		Location productIdSet = new Location(new Namespace("sets","product_info_sets"),"productIds");
		SetUpdate su = new SetUpdate().add(String.valueOf(p.productId));
		UpdateSet updateSet = new UpdateSet.Builder(productIdSet, su).build();
		client.execute(updateSet);
		System.out.println("productset updated");
	}
		
	public String toString() {
		return String.format("Product %d: %s. Aantal: %s, Eenheid: %d",productId,productOmschrijving,inhoudAantal,eenheidId);
	}
}
