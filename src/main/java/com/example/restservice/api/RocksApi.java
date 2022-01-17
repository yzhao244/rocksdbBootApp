package com.example.restservice.api;

import com.example.restservice.repository.KeyValueRepository;

import lombok.var;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/rocks")
public class RocksApi {

  private final KeyValueRepository<String, String, String> rocksDB;

  public RocksApi(KeyValueRepository<String, String, String> rocksDB) {
    this.rocksDB = rocksDB;
  }

  @PostMapping(value = "/{key}", consumes = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> save(@PathVariable("key") String key, @RequestBody String value) {
    log.info("RocksApi.save");
    rocksDB.save(key, value);
    return ResponseEntity.ok(value);
  }
  
  @PostMapping(value = "/columnfamilies/{cf}/key/{key}", consumes = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> saveWithCF(@PathVariable("cf") String cf, 
		  @PathVariable("key") String key, @RequestBody String value) {
    log.info("RocksApi.save");
    rocksDB.save(cf, key, value);
    return ResponseEntity.ok(value);
  }

  @GetMapping(value = "/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> find(@PathVariable("key") String key) {
    log.info("RocksApi.find");
    var result = rocksDB.find(key);
    if(result == null) return ResponseEntity.noContent().build();
    return ResponseEntity.ok(result);
  }
  
  @GetMapping(value = "/ttl/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> findAfterttl(@PathVariable("key") String key) {
    log.info("RocksApi.find");
    var result = rocksDB.findAfterttl(key);
    if(result == null) return ResponseEntity.noContent().build();
    return ResponseEntity.ok(result);
  }
  
  @GetMapping(value = "/columnfamilies/{cf}/key/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> findWithCF(@PathVariable("cf") String cf, 
		  @PathVariable("key") String key) {
    log.info("RocksApi.findWithCF");
    var result = rocksDB.find(cf, key);
    if(result == null) return ResponseEntity.noContent().build();
    return ResponseEntity.ok(result);
  }
  
  @GetMapping(value = "/columnfamilies/{cf}/ttl/key/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> findWithCFAfterttl(@PathVariable("cf") String cf, 
		  @PathVariable("key") String key) {
    log.info("RocksApi.findWithCFAfterttl");
    var result = rocksDB.findAfterttl(cf, key);
    if(result == null) return ResponseEntity.noContent().build();
    return ResponseEntity.ok(result);
  }

  @DeleteMapping(value = "/{key}")
  public ResponseEntity<String> delete(@PathVariable("key") String key) {
    log.info("RocksApi.delete");
    rocksDB.delete(key);
    return ResponseEntity.ok(key);
  }
}
