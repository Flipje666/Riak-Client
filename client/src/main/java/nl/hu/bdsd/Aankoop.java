package nl.hu.bdsd;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.CounterUpdate;
import com.basho.riak.client.api.commands.datatypes.MapUpdate;
import com.basho.riak.client.api.commands.datatypes.RegisterUpdate;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateMap;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;

public class Aankoop {
	public int aankoopId;
    public int klantId;
    public int filiaalId;
    public int productId;
    public Date datum;
    public int aantal;
    
	public void storeAankoop(Aankoop a, RiakClient client) throws ExecutionException, InterruptedException {
		Location aankoopObjectLocation = new Location(new Namespace("maps","aankopen"), String.valueOf(a.aankoopId));
		System.out.println("loc");
		RegisterUpdate ru1 = new RegisterUpdate(Integer.toString(a.aankoopId));
		System.out.println("ru1");
		RegisterUpdate ru2 = new RegisterUpdate(Integer.toString(a.klantId));
		System.out.println("ru2");
		RegisterUpdate ru3 = new RegisterUpdate(Integer.toString(a.filiaalId));
		System.out.println("ru3");
		RegisterUpdate ru4 = new RegisterUpdate(Integer.toString(a.productId));
		System.out.println("ru4");
		RegisterUpdate ru5 = new RegisterUpdate(a.datum.toString());
		System.out.println("ru5");
		CounterUpdate cu1 = new CounterUpdate(a.aantal);
		System.out.println("cu1");
		MapUpdate mu = new MapUpdate()
				.update("aankoopId",ru1)
				.update("klantId", ru2)
				.update("filiaalId", ru3)
				.update("productId",ru4)
				.update("datum", ru5)
				.update("aantal", cu1);
		System.out.println("mu");
		UpdateMap updateMap = new UpdateMap.Builder(aankoopObjectLocation,mu).build();
		client.executeAsync(updateMap);
		System.out.println("exec 1");
		StoreValue store = new StoreValue.Builder(a).withLocation(aankoopObjectLocation).build();
		client.executeAsync(store);
		System.out.println("exec 2");
		
		
		Location aankoopIdSet = new Location(new Namespace("sets","aankoop_info_sets"),"aankoopIds");
		SetUpdate su = new SetUpdate().add(String.valueOf(a.aankoopId));
		UpdateSet updateSet = new UpdateSet.Builder(aankoopIdSet, su).build();
		client.execute(updateSet);
	}
    
    public String toString() {
	   return String.format("Aankoop: %d, klantId: %d, filiaalId: %d, productId: %d, datum: %s, aantal: %d", aankoopId,klantId,filiaalId,productId,datum.toString(),aantal);
   }
}
