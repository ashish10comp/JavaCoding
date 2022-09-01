package com.practice.concurrency.reader_writer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Key {
	private Integer id;

	public Key(Integer id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object object) {
		if(!(object instanceof Key)) {
			return false;
		}
		Key key = ((Key) object);
		return (this.id.intValue() == key.id.intValue());
	}

	@Override
	public int hashCode() {
		return this.id.intValue();
	}

	@Override
	public String toString() {
		return String.valueOf(id);
	}
}

class Value {
	private String str;

	public Value(String str) {
		this.str = str;
	}

	@Override
	public String toString() {
		return str;
	}
}

class KeyValuePair {
	private Key key;
	private Value value;

	public KeyValuePair(Key key, Value value) {
		this.key = key;
		this.value = value;
	}

	public Key getKey() {
		return key;
	}

	public Value getValue() {
		return value;
	}
}

class DataStructure {
	private Semaphore readerTokens;
	private ReentrantReadWriteLock lock;
	private Map<Key, Value> valueByKey;

	public DataStructure(int maximumParallelReaderCount) {
		this.readerTokens = new Semaphore(maximumParallelReaderCount);
		this.lock = new ReentrantReadWriteLock(true);
		this.valueByKey = new HashMap<>();
	}

	public Value read(Key key) throws InterruptedException {
		Value value = null;
		try {
			readerTokens.acquire();
			lock.readLock().lock();
			value = valueByKey.get(key);
		} finally {
			lock.readLock().unlock();
			readerTokens.release();
		}
		return value;
	}

	public void write(Key key, Value value) {
		lock.writeLock().lock();
		valueByKey.put(key, value);
		lock.writeLock().unlock();
	}
}

class Reader implements Callable<Value> {
	private Key key;
	private DataStructure dataStructure;

	public Reader(Key key, DataStructure dataStructure) {
		this.key = key;
		this.dataStructure = dataStructure;
	}

	@Override
	public Value call() {
		Value value = null;
		try {
			value = dataStructure.read(key);
		} catch(InterruptedException e) {
			System.out.println("InterruptedException = " + e);
		}
		return value;
	}
}

class Writer implements Runnable {
	private Key key;
	private Value value;
	private DataStructure dataStructure;

	public Writer(Key key, Value value, DataStructure dataStructure) {
		this.key = key;
		this.value = value;
		this.dataStructure = dataStructure;
	}

	@Override
	public void run() {
		dataStructure.write(key, value);
	}
}

class ReaderWriter {
	public static void main(String[] args) {
		int count = 1000, maximumParallelReaderCount = 5, readerThreadCount = count, writerThreadCount = count;
		Key key = null;
		Value value = null;
		DataStructure dataStructure = new DataStructure(maximumParallelReaderCount);
		ExecutorService readerExecutorService = Executors.newFixedThreadPool(readerThreadCount);
		ExecutorService writerExecutorService = Executors.newFixedThreadPool(writerThreadCount);
		Map<KeyValuePair, Future<Value>> readValueByWriter = new HashMap<>();
		for(int id = 1; id <= count; id++) {
			key = new Key(id);
			value = new Value(String.valueOf((int) (10 * Math.random())));
			writerExecutorService.submit(new Writer(key, value, dataStructure));
			readValueByWriter.put(new KeyValuePair(key, value), readerExecutorService.submit(new Reader(key, dataStructure)));
			value = new Value(String.valueOf(id));
			writerExecutorService.submit(new Writer(key, value, dataStructure));
		}
		try {
			for(Map.Entry<KeyValuePair, Future<Value>> entry : readValueByWriter.entrySet()) {
				key = entry.getKey().getKey();
				value = entry.getKey().getValue();
				System.out.println("Key = " + key + ", Expected Value = " + value + ", Actual Value = " + entry.getValue().get());
			}
		} catch(ExecutionException e) {
			System.out.println("ExecutionException = " + e);
		} catch(InterruptedException e) {
			System.out.println("InterruptedException = " + e);
		}
	}
}