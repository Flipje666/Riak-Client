package nl.hu.bdsd;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Aankoop {
	public int aankoopId_i;
    public int klantId_i;
    public int filiaalId_i;
    public int productId_i;
    public Date datum_dt;
    public int aantal_i;
    
	public void storeAankoop(Aankoop a, RiakClient client) throws ExecutionException, InterruptedException {
		Location aankoopObjectLocation = new Location(new Namespace("aankopen","aankopen"), String.valueOf(a.aankoopId_i));
		RiakObject obj = new RiakObject()
				.setContentType("application/json")
				.setValue(BinaryValue.create("{\"aankoopId_i\":"+
						a.aankoopId_i+",\"klantId_i\":"+
						a.klantId_i+",\"filiaalId_i\":"+
						a.filiaalId_i+",\"productId_i\":"+
						a.productId_i+",\"datum_dt\":\""+
						a.datum_dt+"\",\"aantal_i\":"+
						a.aantal_i));
//		RegisterUpdate ru1 = new RegisterUpdate(Integer.toString(a.aankoopId_i));
//		RegisterUpdate ru2 = new RegisterUpdate(Integer.toString(a.klantId_i));
//		RegisterUpdate ru3 = new RegisterUpdate(Integer.toString(a.filiaalId_i));
//		RegisterUpdate ru4 = new RegisterUpdate(Integer.toString(a.productId_i));
//		RegisterUpdate ru5 = new RegisterUpdate(a.datum_dt.toString());
//		CounterUpdate cu1 = new CounterUpdate(a.aantal_i);
//		MapUpdate mu = new MapUpdate()
//				.update("aankoopId_i",ru1)
//				.update("klantId_i", ru2)
//				.update("filiaalId_i", ru3)
//				.update("productId_i",ru4)
//				.update("datum_dt", ru5)
//				.update("aantal_i", cu1);
//		UpdateMap updateMap = new UpdateMap.Builder(aankoopObjectLocation,mu).build();
//		client.executeAsync(updateMap);
		StoreValue store = new StoreValue.Builder(obj).withLocation(aankoopObjectLocation).build();
		client.executeAsync(store);

		Location aankoopIdSet = new Location(new Namespace("sets","aankoop_info_sets"),"aankoopIds");
		SetUpdate su = new SetUpdate().add(String.valueOf(a.aankoopId_i));
		UpdateSet updateSet = new UpdateSet.Builder(aankoopIdSet, su).build();
		client.executeAsync(updateSet);
	}
    
    public String toString() {
	   return String.format("Aankoop: %d, klantId_i: %d, filiaalId_i: %d, productId_i: %d, datum_dt: %s, aantal_i: %d", aankoopId_i, klantId_i, filiaalId_i, productId_i, datum_dt.toString(), aantal_i);
   }
}
