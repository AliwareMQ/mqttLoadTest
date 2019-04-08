/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.jk5.mqtt;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class StatService implements Runnable {
    private static final int FREQUENCY_OF_SAMPLING = 1000;
    private static final String[] STAT_TIME_MAX_DESC = new String[] {
        "[<=0ms]", "[0~10ms]", "[10~50ms]", "[50~100ms]", "[100~200ms]", "[200~500ms]", "[500ms~1s]", "[1~2s]", "[2~3s]", "[3~4s]", "[4~5s]", "[5~10s]", "[10s~]",
    };
    private int printTPSInterval = 60 * 1;
    private long lastPrintTimestamp = System.currentTimeMillis();

    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isStopped = false;
    private volatile long statDelayTimeMax = 0;
    private volatile AtomicLong[] statMessageDelayTime;
    private String statName;
    private final Thread thread;

    public StatService(String statName, int printTPSInterval) {
        this.statName = statName;
        this.thread = new Thread(this, statName);
        this.printTPSInterval = printTPSInterval;
        this.initDispatchMessageDelayTime();
    }

    public void setStatDelayTimeMax(long value) {
        final AtomicLong[] times = this.statMessageDelayTime;

        if (null == times)
            return;

        // us
        if (value <= 0) {
            times[0].incrementAndGet();
        } else if (value < 10) {
            times[1].incrementAndGet();
        } else if (value < 50) {
            times[2].incrementAndGet();
        } else if (value < 100) {
            times[3].incrementAndGet();
        } else if (value < 200) {
            times[4].incrementAndGet();
        } else if (value < 500) {
            times[5].incrementAndGet();
        } else if (value < 1000) {
            times[6].incrementAndGet();
        }
        // 2s
        else if (value < 2000) {
            times[7].incrementAndGet();
        }
        // 3s
        else if (value < 3000) {
            times[8].incrementAndGet();
        }
        // 4s
        else if (value < 4000) {
            times[9].incrementAndGet();
        }
        // 5s
        else if (value < 5000) {
            times[10].incrementAndGet();
        }
        // 10s
        else if (value < 10000) {
            times[11].incrementAndGet();
        } else {
            times[12].incrementAndGet();
        }

        if (value > this.statDelayTimeMax) {
            this.lock.lock();
            this.statDelayTimeMax =
                value > this.statDelayTimeMax ? value : this.statDelayTimeMax;
            this.lock.unlock();
        }
    }

    @Override
    public void run() {
        while (!this.isStopped) {
            try {
                Thread.sleep(FREQUENCY_OF_SAMPLING);
                this.printTps();
            } catch (Exception e) {
            }
        }
    }

    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        this.isStopped = true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        sb.append("\tstatDelay: " + this.dispatchMessageDelayTimeToString() + "\r\n");
        sb.append("\tstatDelayMax: " + this.statDelayTimeMax + "\r\n");
        return sb.toString();
    }

    private AtomicLong[] initDispatchMessageDelayTime() {
        AtomicLong[] next = new AtomicLong[13];
        for (int i = 0; i < next.length; i++) {
            next[i] = new AtomicLong(0);
        }
        AtomicLong[] old = this.statMessageDelayTime;
        this.statMessageDelayTime = next;
        return old;
    }

    private String dispatchMessageDelayTimeToString() {
        final AtomicLong[] times = this.statMessageDelayTime;
        if (null == times)
            return null;

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times.length; i++) {
            long value = times[i].get();
            sb.append(String.format("%s:%d", STAT_TIME_MAX_DESC[i], value));
            sb.append(" ");
        }

        return sb.toString();
    }

    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + printTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();
            final AtomicLong[] dispatchTime = this.initDispatchMessageDelayTime();
            if (null == dispatchTime)
                return;
            final StringBuilder dispatchSb = new StringBuilder();
            long totalDispatch = 0;
            for (int i = 0; i < dispatchTime.length; i++) {
                long value = dispatchTime[i].get();
                totalDispatch += value;
                dispatchSb.append(String.format("%s:%d", STAT_TIME_MAX_DESC[i], value));
                dispatchSb.append(" ");
            }
            System.out.printf("[%s] TotalNum %d, DelayTime %s \n", statName, totalDispatch, dispatchSb.toString());
        }
    }
}
