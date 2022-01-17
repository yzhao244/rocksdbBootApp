package com.example.restservice.repository;

import lombok.var;
import lombok.extern.slf4j.Slf4j;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

@Slf4j
@Repository
public class RocksDBRepositoryImpl implements KeyValueRepository<String, String, String> {

  private final static String NAME = "first-db";
  File dbDir;
  //RocksDB db;
  TtlDB db;
  TtlDB defaultDB;
  HashMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<String, ColumnFamilyHandle>();
  @PostConstruct
  void initialize() {
    RocksDB.loadLibrary();
    final Options options = new Options();    
    options.setCreateMissingColumnFamilies(true);
    options.setCreateIfMissing(true);
    dbDir = new File("/tmp/rocks-db", NAME);
    try {
      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());
      //db = RocksDB.open(options, dbDir.getAbsolutePath());
      log.info("rocksdb path : " + dbDir.getAbsolutePath().toString());
      db = TtlDB.open(options, dbDir.getAbsolutePath(), 60, false);
      //defaultDB = TtlDB.open(options, dbDir.getAbsolutePath(), 60, false);
    } catch(IOException | RocksDBException ex) {
      log.error("Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
        ex.getCause(), ex.getMessage(), ex.getStackTrace());
    }
    log.info("RocksDB initialized and ready to use");
    //db.close();
  }

  @Override
  public synchronized void save(String key, String value) {
    log.info("save");
    try {
    	db.put(key.getBytes(), value.getBytes());
    } catch (RocksDBException e) {
      log.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }
  
  @Override
  public synchronized void save(String cf, String key, String value) {
    log.info("save key with column families");
    try {
      ColumnFamilyHandle columnFamilyHandle = db.createColumnFamilyWithTtl(
              new ColumnFamilyDescriptor(cf.getBytes()), 1);
      columnFamilyHandleMap.put(cf, columnFamilyHandle);
      db.put(columnFamilyHandle, key.getBytes(), value.getBytes());
    } catch (RocksDBException e) {
      log.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }
  
  @Override
  public synchronized String find(String key) {
    log.info("find");
    String result = null;
    try {
      
      var bytes = db.get(key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }

  @Override
  public synchronized String findAfterttl(String key) {
    log.info("find");
    String result = null;
    try {
      
      Thread.sleep(1000);
      db.compactRange();      
      var bytes = db.get(key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException | InterruptedException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }
  
  @Override
  public synchronized String find(String cf, String key) {
    log.info("find");
    String result = null;
    try {
      ColumnFamilyHandle cfHandle = columnFamilyHandleMap.get(cf);
      var bytes = db.get(cfHandle, key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }

  @Override
  public synchronized String findAfterttl(String cf, String key) {
    log.info("find");
    String result = null;
    try {
      
      ColumnFamilyHandle cfHandle = columnFamilyHandleMap.get(cf);
      Thread.sleep(1000);
      //db.compactRange(cfHandle);      
      db.compactRange();
      var bytes = db.get(cfHandle, key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException | InterruptedException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }
  
  /*@Override
  public synchronized String find(String cf, String key) {
    log.info("find with column families");
    String result = null;
    final DBOptions options = new DBOptions();
    options.setCreateMissingColumnFamilies(true);
    options.setCreateIfMissing(true);    
    List<ColumnFamilyDescriptor> cfNames = Arrays.asList(            
            new ColumnFamilyDescriptor(cf.getBytes())
        );
    List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    List<Integer> ttlValues = Arrays.asList(5);
    
    try {
      TtlDB ttlDB = TtlDB.open(options, dbDir.getAbsolutePath(), cfNames, columnFamilyHandleList, ttlValues, false);
      var bytes = ttlDB.get(columnFamilyHandleList.get(0), key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }

  @Override
  public synchronized String findAfterttl(String cf, String key) {
	    log.info("find with column families");
	    String result = null;
	    final DBOptions options = new DBOptions();
	    options.setCreateMissingColumnFamilies(true);
	    options.setCreateIfMissing(true);    
	    List<ColumnFamilyDescriptor> cfNames = Arrays.asList(            
	            new ColumnFamilyDescriptor(cf.getBytes())
	        );
	    
	    List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
	    List<Integer> ttlValues = Arrays.asList(5);
	    
	    try {
	      TtlDB ttlDB = TtlDB.open(options, dbDir.getAbsolutePath(), cfNames, columnFamilyHandleList, ttlValues, false);
	      ttlDB.compactRange();
	      var bytes = ttlDB.get(columnFamilyHandleList.get(0), key.getBytes());
	      if(bytes == null) return null;
	      result = new String(bytes);
	    } catch (RocksDBException e) {
	      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
	    }
	    return result;
  }*/

  @Override
  public synchronized void delete(String key) {
    log.info("delete");
    try {
      db.delete(key.getBytes());
    } catch (RocksDBException e) {
      log.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }
}
