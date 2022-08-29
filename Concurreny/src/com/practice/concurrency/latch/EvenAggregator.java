package com.practice.concurrency.latch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Event implements Runnable {
    private CountDownLatch countDownLatch;

    public Event(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        System.out.println("Event arrived!!");
        countDownLatch.countDown();
    }
}

class Aggregator implements Runnable {
    private long eventCount;
    private CountDownLatch countDownLatch;

    public Aggregator(CountDownLatch countDownLatch) {
        this.eventCount = countDownLatch.getCount();
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        System.out.println("Waiting for " + eventCount + " events to occur!!");
        try {
            countDownLatch.await();
            // some database call
        } catch(InterruptedException e) {
            System.out.println("InterruptedException = " + e);
        }
        System.out.println("Added counter " + eventCount + " in database!!");
    }
}

class EvenAggregator {
    public static void main(String[] args) {
        int eventCount = 10, threadCount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(eventCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        executorService.submit(new Aggregator(countDownLatch));
        while(eventCount > 0) {
            executorService.submit(new Event(countDownLatch));
            eventCount--;
        }
        executorService.shutdown();
    }
}