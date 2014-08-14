package com.activeandroid.runtime;

import android.os.Looper;
import android.os.Process;

import com.activeandroid.util.AALog;

import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by andrewgrosner
 * Date: 12/11/13
 * Contributors:
 * Description: will handle concurrent requests to the DB based on priority
 */
public class DBRequestQueue extends Thread{

    /**
     * Queue of requests
     */
    private final PriorityBlockingQueue<DBRequest> mQueue;

    private boolean mQuit = false;

    /**
     * Creates a queue with the specified name to ID it.
     * @param name
     */
    public DBRequestQueue(String name) {
        super(name);

        mQueue = new PriorityBlockingQueue<DBRequest>();
    }

    @Override
    public void run() {
        Looper.prepare();
        android.os.Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
        DBRequest runnable;
        while (true){
            try{
                runnable = mQueue.take();
            } catch (InterruptedException e){
                if(mQuit){
                    synchronized (mQueue) {
                        mQueue.clear();
                    }
                    return;
                }
                continue;
            }

            try{
                if(AALog.isEnabled()) {
                    AALog.d("DBRequestQueue + " + getName(), "Size is: " + mQueue.size() + " executing:" + runnable.getName());
                }
                runnable.run();
            } catch (Throwable t){
                throw new RuntimeException(t);
            }
        }

    }

    public void add(DBRequest runnable){
        if (!mQueue.contains(runnable)) {
            mQueue.add(runnable);
        }
    }

    /**
     * Cancels the specified request.
     * @param runnable
     */
    public void cancel(DBRequest runnable){
        if (mQueue.contains(runnable)) {
            mQueue.remove(runnable);
        }
    }

    /**
     * Cancels all requests by a specific tag
     * @param tag
     */
    public void cancel(String tag){
        synchronized (mQueue){
            Iterator<DBRequest> it = mQueue.iterator();
            while(it.hasNext()){
                DBRequest next = it.next();
                if(next.getName().equals(tag)){
                    it.remove();
                }
            }
        }
    }

    /**
     * Quits this process
     */
    public void quit(){
        mQuit = true;
        interrupt();
    }
}
