package com.practice.concurrency.producer_consumer;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;

interface Task {
    public void execute();
}

class ServiceCall {
    private String name;

    public ServiceCall(String name) {
        this.name = name;
    }

    public void call() {
        System.out.println("Service Called = " + name);
    }

    public String getname() {
    	return this.name;
    }
}

class PrintTask implements Task {
    private String name;

    public PrintTask(String name) {
        this.name = name;
    }

    @Override
    public void execute() {
        System.out.println("Executed Print Task = " + name);
    }
}

class PushNotificationTask implements Task {
    private ServiceCall serviceCall;

    public PushNotificationTask(ServiceCall serviceCall) {
        this.serviceCall = serviceCall;
    }

    @Override
    public void execute() {
        serviceCall.call();
        System.out.println("Executed Push Notification Task = " + serviceCall.getname());
    }
}

class DelayedTaskCallable implements Callable<Long> {
    private Task task;

    public DelayedTaskCallable(Task task) {
        this.task = task;
    }

    @Override
    public Long call() {
        task.execute();
        return Instant.now().getEpochSecond();
    }
}

class DelayedTask implements Comparable<DelayedTask> {
    private long epochRuntime;
    private FutureTask<DelayedTaskCallable> futureDelayedTaskCallable;

    public DelayedTask(long epochRuntime, FutureTask<DelayedTaskCallable> futureDelayedTaskCallable) {
        this.epochRuntime = epochRuntime;
        this.futureDelayedTaskCallable = futureDelayedTaskCallable;
    }

    public int compareTo(DelayedTask delayedTask) {
        if(this.epochRuntime < delayedTask.epochRuntime) {
            return -1;
        }
        if(this.epochRuntime < delayedTask.epochRuntime) {
            return 1;
        }
        return 0;
    }

    public long getEpochRuntime() {
        return epochRuntime;
    }

    public FutureTask<DelayedTaskCallable> getFutureDelayedTaskCallable() {
        return futureDelayedTaskCallable;
    }
}

class DelayedTaskRunnable implements Runnable {
    private boolean isStopped;
    private int taskRunnableId;
    private Thread thread;
    private BlockingQueue<DelayedTask> blockingQueue;

    public DelayedTaskRunnable(int taskRunnableId, BlockingQueue<DelayedTask> blockingQueue) {
        this.isStopped = false;
        this.taskRunnableId = taskRunnableId;
        this.thread = null;
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();
        DelayedTask delayedTask = null;
        while(!isStopped()) {
            while(!blockingQueue.isEmpty() && (Instant.now().getEpochSecond() < blockingQueue.peek().getEpochRuntime())) {
                continue;
            }
            try {
                delayedTask = blockingQueue.take();
                while(Instant.now().getEpochSecond() < delayedTask.getEpochRuntime()) {
                    continue;
                }
                new Thread(delayedTask.getFutureDelayedTaskCallable()).start();
                System.out.println("Executed Task Runnable = " + taskRunnableId);
            } catch(InterruptedException e) {
                System.out.println("InterruptedException = " + e);
            }
        }
        this.thread.interrupt();
    }

    public synchronized boolean isStopped() {
        return this.isStopped;
    }

    public synchronized void stopTaskRunnable() {
        this.isStopped = true;
    }
}

class DelayedTaskConsumer {
    private List<DelayedTaskRunnable> delayedTaskRunnables;

    public DelayedTaskConsumer(int threadCount, BlockingQueue<DelayedTask> blockingQueue) {
        this.delayedTaskRunnables = new ArrayList<>();
        while(threadCount > 0) {
            delayedTaskRunnables.add(new DelayedTaskRunnable(threadCount, blockingQueue));
            threadCount--;
        }
        for(DelayedTaskRunnable delayedTaskRunnable : delayedTaskRunnables) {
            new Thread(delayedTaskRunnable).start();
        }
    }

    public void stopDelayedTaskConsumer() {
        for(DelayedTaskRunnable delayedTaskRunnable : delayedTaskRunnables) {
            delayedTaskRunnable.stopTaskRunnable();
        }
    }
}

class ProducerConsumer {
    public static void main(String[] args) throws IOException {
        List<DelayedTask> delayedTasks = new ArrayList<>();
        BlockingQueue<DelayedTask> blockingQueue = new PriorityBlockingQueue<>();
        DelayedTaskConsumer delayedTaskConsumer = new DelayedTaskConsumer(5, blockingQueue);
        try {
        	for(int id = 0; id < 10; id++) {
            	if((id % 2) == 0) {
            		delayedTasks.add(new DelayedTask(Instant.now().plus(10, ChronoUnit.SECONDS).getEpochSecond(), new FutureTask(new DelayedTaskCallable(new PrintTask("PrintTask" + id)))));
            	} else {
            		if((id % 3) == 0) {
            			delayedTasks.add(new DelayedTask(Instant.now().plus(15, ChronoUnit.SECONDS).getEpochSecond(), new FutureTask(new DelayedTaskCallable(new PushNotificationTask(new ServiceCall("Mobile Service Call" + id))))));
            		} else {
            			delayedTasks.add(new DelayedTask(Instant.now().plus(20, ChronoUnit.SECONDS).getEpochSecond(), new FutureTask(new DelayedTaskCallable(new PushNotificationTask(new ServiceCall("Email Service Call" + id))))));
            		}
            	}
            	Thread.sleep(1000);
            }
            for(DelayedTask delayedTask : delayedTasks) {
                blockingQueue.put(delayedTask);
            }
            for(DelayedTask delayedTask : delayedTasks) {
                System.out.println("Delayed Task Done at " + delayedTask.getFutureDelayedTaskCallable().get());
            }
            delayedTaskConsumer.stopDelayedTaskConsumer();
        } catch(ExecutionException e) {
            System.out.println("ExecutionException = " + e);
        } catch(InterruptedException e) {
            System.out.println("InterruptedException = " + e);
        }
    }
}