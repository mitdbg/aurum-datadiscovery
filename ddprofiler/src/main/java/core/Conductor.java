package core;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import analysis.modules.EntityAnalyzer;
import core.config.ProfilerConfig;
import metrics.Metrics;
import store.Store;

public class Conductor {

  final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());

  private ProfilerConfig pc;
  private File errorLogFile;

  private BlockingQueue<WorkerTask> taskQueue;
  private BlockingQueue<WorkerSubTask> subTaskQueue;
  private List<Worker> activeWorkers;
  private List<Thread> workerPool;
  private BlockingQueue<WorkerTaskResult> results;
  private BlockingQueue<ErrorPackage> errorQueue;

  private Store store;

  private Thread consumer;
  private ErrorConsumer runnable;
  // Cached entity analyzers (expensive initialization)
  private Map<String, EntityAnalyzer> cachedEntityAnalyzers;

  // Metrics
  private int totalTasksSubmitted = 0;
  private int totalFailedTasks = 0;
  private AtomicInteger totalProcessedTasks = new AtomicInteger();
  private AtomicInteger totalColumns = new AtomicInteger();
  private Meter m;
  public static Meter recordsPerSecond;

  public Conductor(ProfilerConfig pc, Store s) {
    this.pc = pc;
    this.store = s;
    this.taskQueue = new LinkedBlockingQueue<>();
    this.subTaskQueue = new LinkedBlockingQueue<>();
    this.results = new LinkedBlockingQueue<>();
    this.errorQueue = new LinkedBlockingQueue<>();

    int numWorkers = pc.getInt(ProfilerConfig.NUM_POOL_THREADS);
    this.workerPool = new ArrayList<>();
    this.activeWorkers = new ArrayList<>();

    // Entity analyzer is cached in Textual Analyzer
//    List<TokenNameFinderModel> modelList = new ArrayList<>();
//    List<String> modelNameList = new ArrayList<>();
//    EntityAnalyzer first = new EntityAnalyzer();
//    modelList = first.getCachedModelList();
//    modelNameList = first.getCachedModelNameList();
//    EntityAnalyzer cached = new EntityAnalyzer(modelList, modelNameList);

    this.createTaskProducers(numWorkers);
    this.createTaskConsumers(numWorkers);
    this.createMainConsumer();

    // Metrics
    m = Metrics.REG.meter(name(Conductor.class, "tasks", "per", "sec"));
    recordsPerSecond = Metrics.REG.meter(name(Conductor.class, "records", "per", "sec"));
  }

  private void createTaskProducers(int numWorkers) {
    for (int i = 0; i < numWorkers; i++) {
      String name = "Producer-" + new Integer(i).toString();
      TaskProducer worker = new TaskProducer(this, pc, name);
      workerPool.add(new Thread(worker, name));
      activeWorkers.add(worker);
    }
  }

  private void createTaskConsumers(int numWorkers) {
    for (int i = 0; i < numWorkers; i++) {
      String name = "Consumer-" + new Integer(i).toString();
      TaskConsumer worker = new TaskConsumer(this, pc, name, store);
      workerPool.add(new Thread(worker, name));
      activeWorkers.add(worker);
    }
  }

  private void createMainConsumer() {
    this.runnable = new ErrorConsumer();
    this.consumer = new Thread(runnable);
    String errorLogFileName = pc.getString(ProfilerConfig.ERROR_LOG_FILE_NAME);
    this.errorLogFile = new File(errorLogFileName);
  }

  public void start() {
    this.store.initStore();
    this.consumer.start();
  }

  public void stop() {
    this.runnable.stop();
    try {
      this.consumer.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean submitTask(WorkerTask task) {
    totalTasksSubmitted++;
    return taskQueue.add(task);
  }

  public WorkerTask pullTask() {
    try {
      return taskQueue.poll(500, TimeUnit.MILLISECONDS);
    } catch(InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }

  public boolean submitSubTask(WorkerSubTask task) {
    return subTaskQueue.add(task);
  }

  public WorkerSubTask pullSubTask() {
    try {
      return subTaskQueue.poll(500, TimeUnit.MILLISECONDS);
    } catch(InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void submitError(String log) {
    try {
      errorQueue.put(new ErrorPackage(log));
    } catch(InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public int approxQueueLength() {
	  return subTaskQueue.size();
  }

  public boolean isTherePendingWork() {
    return this.totalProcessedTasks.get() < this.totalTasksSubmitted;
  }

  public List<WorkerTaskResult> consumeResults() {
    List<WorkerTaskResult> availableResults = new ArrayList<>();
    WorkerTaskResult wtr = null;
    do {
      try {
        wtr = results.poll(500, TimeUnit.MILLISECONDS);
        if (wtr != null) {
          availableResults.add(wtr);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (wtr != null);
    return availableResults;
  }

  public void notifyProcessedTask(int numCols) {
    totalProcessedTasks.incrementAndGet();
    m.mark();
    LOG.info(" {}/{} ", totalProcessedTasks, totalTasksSubmitted);
    LOG.info(" Failed tasks: {} ", totalFailedTasks);
    totalColumns.addAndGet(numCols);
    LOG.info("Added: {} cols, total processed: {} ", numCols, totalColumns);
    LOG.info("");
  }

  class ErrorConsumer implements Runnable {

    private boolean doWork = true;

    public void stop() { doWork = false; }

    @Override
    public void run() {

      // Start workers
      for (Thread worker : workerPool) {
        worker.start();
      }

      while (doWork) {

        ErrorPackage ep;
        try {
          ep = errorQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (ep != null) {
            String msg = ep.getErrorLog();
            Utils.appendLineToFile(errorLogFile, msg);
            LOG.warn(msg);
            totalProcessedTasks
                .incrementAndGet(); // other processed/failed task
            totalFailedTasks++;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // Stop workers
      for (Worker w : activeWorkers) {
        w.stop();
      }

      LOG.info("ErrorConsumer stopping");
    }
  }
}
