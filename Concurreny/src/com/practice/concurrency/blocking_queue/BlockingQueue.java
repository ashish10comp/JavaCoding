package com.practice.concurrency.blocking_queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;

interface Task {
    public void execute(int id);
}

class Monitor {}

class PrintTask implements Task {
    private String text;

    public PrintTask(String text) {
        this.text = text;
    }

    @Override
    public void execute(int id) {
        System.out.println("Consumer " + id + " executes PrintTask = " + text);
    }
}

class LogTask implements Task {
    private String text;

    public LogTask(String text) {
        this.text = text;
    }

    @Override
    public void execute(int id) {
        System.out.println("Consumer " + id + " executes LogTask = " + text);
    }
}

class Consumer implements Runnable {
    private int id;
    private Monitor monitor;
    private Semaphore semaphore;
    private Queue<Task> queue;

    public Consumer(int id, Monitor monitor, Semaphore semaphore, Queue<Task> queue) {
        this.id = id;
        this.monitor = monitor;
        this.semaphore = semaphore;
        this.queue = queue;
    }

    @Override
    public void run() {
        Task task = null;
        while(true) {
        	try {
        		semaphore.acquire();
            	synchronized(monitor) {
            		try {
                        while(queue.isEmpty()) {
                        	monitor.wait();
                        }
                        task = queue.poll();
                        task.execute(id);
                    } catch(InterruptedException e) {
                        System.out.println("InterruptedException = " + e);
                    } finally {
                        monitor.notifyAll();
                    }
            	}
        	} catch(InterruptedException e) {
                System.out.println("InterruptedException = " + e);
            } finally {
        		semaphore.release();
        	}
        }
    }
}

class PrintTaskProducer implements Runnable {
    private int id;
    private int queueCapacity;
    private Monitor monitor;
    private Queue<Task> queue;

    public PrintTaskProducer(int id, int queueCapacity, Monitor monitor, Queue<Task> queue) {
        this.id = id;
        this.queueCapacity = queueCapacity;
        this.monitor = monitor;
        this.queue = queue;
    }

    @Override
    public void run() {
        Task task = new PrintTask(String.valueOf(id));
        synchronized(monitor) {
        	try {
                while(queue.size() == queueCapacity) {
                    monitor.wait();
                }
                queue.add(task);
        	} catch(InterruptedException e) {
        		System.out.println("InterruptedException = " + e);
        	} finally {
        		monitor.notifyAll();
        	}
        }
    }
}

class LogTaskProducer implements Runnable {
    private int id;
    private int queueCapacity;
    private Monitor monitor;
    private Queue<Task> queue;

    public LogTaskProducer(int id, int queueCapacity, Monitor monitor, Queue<Task> queue) {
        this.id = id;
        this.queueCapacity = queueCapacity;
        this.monitor = monitor;
        this.queue = queue;
    }

    @Override
    public void run() {
        Task task = new LogTask(String.valueOf(id));
        synchronized(monitor) {
        	try {
                while(queue.size() == queueCapacity) {
                    monitor.wait();
                }
                queue.add(task);
        	} catch(InterruptedException e) {
        		System.out.println("InterruptedException = " + e);
        	} finally {
        		monitor.notifyAll();
        	}
        }
    }
}

class BlockingQueue {
    public static void main(String[] args) throws InterruptedException {
        int queueCapacity = 10, consumerThreadCount = 5, producerThreadCount = 10, consumerSemaphoreCount = 3;
        Monitor monitor = new Monitor();
        Semaphore semaphore = new Semaphore(consumerSemaphoreCount);
        Queue<Task> queue = new LinkedList<>();
        List<Thread> consumerThreads = new ArrayList<>(), producerThreads = new ArrayList<>();
        while(consumerThreadCount > 0) {
            consumerThreads.add(new Thread(new Consumer(consumerThreadCount, monitor, semaphore, queue)));
            consumerThreadCount--;
        }
        while(producerThreadCount > 0) {
            producerThreads.add(new Thread(new PrintTaskProducer(producerThreadCount, queueCapacity, monitor, queue)));
            producerThreads.add(new Thread(new LogTaskProducer(producerThreadCount, queueCapacity, monitor, queue)));
            producerThreadCount--;
        }
        for(Thread consumerThread : consumerThreads) {
            consumerThread.start();
        }
        for(Thread producerThread : producerThreads) {
            producerThread.start();
        }
    }
}