// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;

import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;

final class TextImporter {

  private static final Logger LOG = LoggerFactory.getLogger(TextImporter.class);

  /** Prints usage and exits with the given retval.  */
  static void usage(final ArgP argp, final int retval) {
    System.err.println("Usage: import path [more paths]");
    System.err.print(argp.usage());
    System.err.println("This tool can directly read gzip'ed input files.");
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
    //解析命令行参数
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addAutoMetricFlag(argp);
    argp.addOption("--skip-errors", "Whether or not to skip exceptions "
        + "during processing");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, 1);
    } else if (args.length < 1) {
      usage(argp, 2);
    }
    
    // get a config object
    Config config = CliOptions.getConfig(argp);

    final TSDB tsdb = new TSDB(config);
    final boolean skip_errors = argp.has("--skip-errors");
    //检测hbase表是否存在
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;
    try {
      int points = 0;
      final long start_time = System.nanoTime();
      for (final String path : args) {
        //读取指定的数据文件并导入
        points += importFile(tsdb.getClient(), tsdb, path, skip_errors);
      }
      final double time_delta = (System.nanoTime() - start_time) / 1000000000.0;
      LOG.info(String.format("Total: imported %d data points in %.3fs"
                             + " (%.1f points/s)",
                             points, time_delta, (points / time_delta)));
      // TODO(tsuna): Figure out something better than just writing to stderr.
      tsdb.collectStats(new StatsCollector("tsd") {
        @Override
        public final void emit(final String line) {
          System.err.print(line);
        }
      });
    } finally {
      try {
        //关闭tsdb实例
        tsdb.shutdown().joinUninterruptibly();
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        System.exit(1);
      }
    }
  }

  static volatile boolean throttle = false;

  private static int importFile(final HBaseClient client,
                                final TSDB tsdb,
                                final String path,
                                final boolean skip_errors) throws IOException {
    final long start_time = System.nanoTime();
    long ping_start_time = start_time;
    //读取指定路径的文件
    final BufferedReader in = open(path);
    String line = null;
    int points = 0;
    try {
      final class Errback implements Callback<Object, Exception> {
        public Object call(final Exception arg) {
          //针对特定异常进行处理，流量异常
          if (arg instanceof PleaseThrottleException) {
            final PleaseThrottleException e = (PleaseThrottleException) arg;
            LOG.warn("Need to throttle, HBase isn't keeping up.", e);
            //将限流状态位更改为true
            throttle = true;
            final HBaseRpc rpc = e.getFailedRpc();
            //如果是PutRequest的异常，进行重试
            if (rpc instanceof PutRequest) {
              client.put((PutRequest) rpc);  // Don't lose edits.
            }
            return null;
          }
          LOG.error("Exception caught while processing file "
                    + path, arg);
          System.exit(2);
          return arg;
        }
        public String toString() {
          return "importFile errback";
        }
      };
      final Errback errback = new Errback();
      LOG.info("reading from file:" + path);
      //读取一行数据，一行对应一个时序点数据
      while ((line = in.readLine()) != null) {
        //按照空格对数据进行切分
        final String[] words = Tags.splitString(line, ' ');
        //检测metric是否合法
        final String metric = words[0];
        if (metric.length() <= 0) {
          if (skip_errors) {
            LOG.error("invalid metric: " + metric);
            LOG.error("error while processing file "
                      + path + " line=" + line + "... Continuing");
            continue;
          } else {
            throw new RuntimeException("invalid metric: " + metric);
          }
        }
        //检测timestamp是否合法
        final long timestamp;
        try {
          timestamp = Tags.parseLong(words[1]);
          if (timestamp <= 0) {
            if (skip_errors) {
              LOG.error("invalid timestamp: " + timestamp);
              LOG.error("error while processing file "
                        + path + " line=" + line + "... Continuing");
              continue;
            } else {
              throw new RuntimeException("invalid timestamp: " + timestamp);
            }
          }
        } catch (final RuntimeException e) {
          if (skip_errors) {
            LOG.error("invalid timestamp: " + e.getMessage());
            LOG.error("error while processing file "
                      + path + " line=" + line + "... Continuing");
            continue;
          } else {
            throw e;
          }
        }
        //检测value是否合法
        final String value = words[2];
        if (value.length() <= 0) {
          if (skip_errors) {
            LOG.error("invalid value: " + value);
            LOG.error("error while processing file "
                      + path + " line=" + line + "... Continuing");
            continue;
          } else {
            throw new RuntimeException("invalid value: " + value);
          }
        }
        
        try {
          //解析该数据点对应的tag
          final HashMap<String, String> tags = new HashMap<String, String>();
          //一个tag 三个字节
          for (int i = 3; i < words.length; i++) {
            if (!words[i].isEmpty()) {
              Tags.parse(tags, words[i]);
            }
          }
          //创建WritableDataPoints对象，底层为IncomingDataPoints对象，
          // 可以记录多个数据点，这些点的metric和tag必须相同
          final WritableDataPoints dp = getDataPoints(tsdb, metric, tags);
          Deferred<Object> d;
          //根据value类型进行数据添加
          if (Tags.looksLikeInteger(value)) {
            d = dp.addPoint(timestamp, Tags.parseLong(value));
          } else {  // floating point value
            d = dp.addPoint(timestamp, Float.parseFloat(value));
          }
          //添加回调
          d.addErrback(errback);
          //记录写入点的个数
          points++;
          //限流操作
          if (points % 1000000 == 0) {
            final long now = System.nanoTime();
            ping_start_time = (now - ping_start_time) / 1000000;
            LOG.info(String.format("... %d data points in %dms (%.1f points/s)",
                                   points, ping_start_time,
                                   (1000000 * 1000.0 / ping_start_time)));
            ping_start_time = now;
          }
          //为true表示进行限流操作
          if (throttle) {
            LOG.info("Throttling...");
            long throttle_time = System.nanoTime();
            try {
              //等待当前IncomingDataPoints中的点全部写入完成，再开始进行写入
              d.joinUninterruptibly();
            } catch (final Exception e) {
              throw new RuntimeException("Should never happen", e);
            }
            throttle_time = System.nanoTime() - throttle_time;
            if (throttle_time < 1000000000L) {
              LOG.info("Got throttled for only " + throttle_time + 
                  "ns, sleeping a bit now");
              try {
                //如果限流时间短，在暂停一段时间
                Thread.sleep(1000); 
              } catch (InterruptedException e) { 
                throw new RuntimeException("interrupted", e); 
              }
            }
            LOG.info("Done throttling...");
            throttle = false;
          }
        } catch (final RuntimeException e) {
          if (skip_errors) {
            LOG.error("Exception: " + e.getMessage());
            LOG.error("error while processing file "
                      + path + " line=" + line + "... Continuing");
            continue;
          } else {
            throw e;
          }
        }
      }
    } catch (RuntimeException e) {
        LOG.error("Exception caught while processing file "
                  + path + " line=[" + line + "]", e);
        throw e;
    } finally {
      in.close();
    }
    final long time_delta = (System.nanoTime() - start_time) / 1000000;
    LOG.info(String.format("Processed %s in %d ms, %d data points"
                           + " (%.1f points/s)",
                           path, time_delta, points,
                           (points * 1000.0 / time_delta)));
    return points;
  }

  /**
   * Opens a file for reading, handling gzipped files.
   * @param path The file to open.
   * @return A buffered reader to read the file, decompressing it if needed.
   * @throws IOException when shit happens.
   */
  private static BufferedReader open(final String path) throws IOException {
    if (path.equals("-")) {
      return new BufferedReader(new InputStreamReader(System.in));
    }

    InputStream is = new FileInputStream(path);
    if (path.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    // I <3 Java's IO library.
    return new BufferedReader(new InputStreamReader(is));
  }

  private static final HashMap<String, WritableDataPoints> datapoints =
    new HashMap<String, WritableDataPoints>();

  private static
    WritableDataPoints getDataPoints(final TSDB tsdb,
                                     final String metric,
                                     final HashMap<String, String> tags) {
    final String key = metric + tags;
    WritableDataPoints dp = datapoints.get(key);
    if (dp != null) {
      return dp;
    }
    dp = tsdb.newDataPoints();
    dp.setSeries(metric, tags);
    dp.setBatchImport(true);
    datapoints.put(key, dp);
    return dp;
  }

}
