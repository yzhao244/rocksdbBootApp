package com.example.restservice.repository;

public interface KeyValueRepository<CF, K, V> {
  void save(K key, V value);
  void save(CF cf, K key, V value);
  V find(K key);
  V findAfterttl(K key);
  V find(CF cf, K key);
  V findAfterttl(CF cf, K key);
  void delete(K key);
}
