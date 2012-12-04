package com.senseidb.gateway.file;

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Joiner;
import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;
import com.linkedin.zoie.impl.indexing.ZoieConfig;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class TTTGateway extends SenseiGateway<String>{

    static Logger logger = Logger.getLogger(TTTGateway.class);

    private Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;

    @Override
    public StreamDataProvider<JSONObject> buildDataProvider(
            DataSourceFilter<String> dataFilter,
            String oldSinceKey,
            ShardingStrategy shardingStrategy,
            Set<Integer> partitions) throws Exception {
        String host = config.get("host");
        if (host == null) host = "localhost";
        return new TTTDataProvider(_versionComparator, host);
    }

    @Override
    public Comparator<String> getVersionComparator() {
        return _versionComparator;
    }

    public static void main(String[] args) throws UnknownHostException {
        /*DB db = new Mongo().getDB("ttt");
        DBCollection tiendas = db.getCollection("tiendas");
        
        DBObject project = o("$project", o("search_id", 1));
        DBObject group = o("$group", j(o("_id", 1), o("maxId", o("$max", "$search_id"))));
        AggregationOutput result = tiendas.aggregate(project, group);
        
        DBObject resp = result.results().iterator().next();
        Integer iMaxId = (Integer)resp.get("maxId");
        int maxId = iMaxId == null ? 0 : iMaxId;
        
        DBCursor cursor = tiendas.find(o("search_id", o("$exists", false))).sort(o("last_update", 1));
        
        for (DBObject obj : cursor) {
            tiendas.update(o("_id", obj.get("_id")), o("$set", o("search_id", ++maxId)));
            // enqueue event obj + searchId
        }*/
    }
    
}


class TTTDataProvider extends StreamDataProvider<JSONObject> {

    private String _offsetT;
    private String _offsetP;
    private final DB db;
    private final LinkedBlockingQueue<DataEvent<JSONObject>> queue;

    public TTTDataProvider(Comparator<String> comparator, String host) throws UnknownHostException {
        super(comparator);
        db = new Mongo(host).getDB("ttt");
        queue = new LinkedBlockingQueue<DataEvent<JSONObject>>();
        new Thread(new Worker()).start();
    }
    
    

    @Override
    public DataEvent<JSONObject> next() {
        DataEvent<JSONObject> event = queue.poll();
        if (event != null) {
            TTTGateway.logger.info("Returning version: " + event.getVersion() + ". Queue size: " + queue.size());
        }
        return event;
    }

    @Override
    public void reset() {
        _offsetT = null;
        _offsetP = null;
    }

    @Override
    public void setStartingOffset(String o) {
    }

    class Worker implements Runnable {
        @Override
        public void run() {
            while (true) {
                TTTGateway.logger.info("Gateway update cycle");
                updateSearchIds();
                indexNewChanges();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            }
        }

        private void indexNewChanges() {
            _offsetT = indexNewChanges("tienda", db.getCollection("tiendas"), _offsetT);
            _offsetP = indexNewChanges("producto", db.getCollection("productos"), _offsetP);
        }

        private String indexNewChanges(String tipo, DBCollection tiendas, String offset) {
            DBCursor cursor = tiendas.find(
                j(
                    o("search_id", o("$exists", true)),
                    o("last_update", o("$gt", offset))
                )
            );
            
            for (DBObject obj : cursor) {
                queue.add(event(tipo, obj, null));
                String lastUpdate = (String)obj.get("last_update");
                if (lastUpdate == null) lastUpdate = "0";
                if (offset == null || lastUpdate.compareTo(offset) > 0) offset = lastUpdate;
            }
            return offset;
        }

        private void updateSearchIds() {
            updateSearchIds("tienda", db.getCollection("tiendas"));
            updateSearchIds("producto", db.getCollection("productos"));
        }
        
        private int maxId = 0;

        private void updateSearchIds(String tipo, DBCollection col) {
            DBObject project = o("$project", o("search_id", 1));
            DBObject group = o("$group", j(o("_id", 1), o("maxId", o("$max", "$search_id"))));
            AggregationOutput result = col.aggregate(project, group);
            
            DBObject resp = result.results().iterator().next();
            Integer iMaxId = (Integer)resp.get("maxId");
            if (iMaxId != null) maxId = Math.max(maxId, iMaxId);
            
            DBCursor cursor = col.find(o("search_id", o("$exists", false))).sort(o("last_update", 1));
            
            for (DBObject obj : cursor) {
                int searchId = ++maxId;
                col.update(o("_id", obj.get("_id")), o("$set", o("search_id", searchId)));
                queue.add(event(tipo, obj, searchId));
            }
        }
    }
    
    DataEvent<JSONObject> event(String tipo, DBObject obj, Integer searchId) {
        JSONObject json = new JSONObject();
        if (searchId == null) {
            searchId = (Integer) obj.get("search_id");
        }
        try {
            json.put("id", searchId);
            json.put("tipo", tipo);
            json.put("_id", obj.get("_id"));
            String content;
            
            if (tipo.equals("tienda")) {
                content = Joiner.on('\n').skipNulls().join(
                        obj.get("nombre"),
                        obj.get("descripcion"),
                        obj.get("direccion"),
                        obj.get("direccion_barrio"),
                        obj.get("rubros"),
                        obj.get("destinatarios"),
                        obj.get("estilos"),
                        obj.get("url"));
            } else {
                DBObject tienda = db.getCollection("tiendas").findOne(o("_id", obj.get("tienda")));
                content = Joiner.on('\n').skipNulls().join(
                        obj.get("nombre"),
                        obj.get("descripcion"),
                        obj.get("categoria"),
                        obj.get("colores"),
                        obj.get("destinatario"),
                        obj.get("estilo"),
                        tienda.get("nombre"),
                        tienda.get("direccion"),
                        tienda.get("direccion_barrio"));                
            }
            json.put("content", content);
        } catch (JSONException e) {
            TTTGateway.logger.error("Failed setting event data", e);
        }
        
        return new DataEvent<JSONObject>(json, (String) obj.get("last_update"));
    }
    
    static DBObject o(String k, Object v) {
        return new BasicDBObject(k,v);
    }
    static DBObject j(DBObject... os) {
        BasicDBObject obj = new BasicDBObject();
        for (DBObject o : os) {
            obj.putAll(o);
        }
        return obj;
    }
}

