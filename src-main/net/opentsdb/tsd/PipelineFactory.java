// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.concurrent.ThreadFactory;
import net.opentsdb.auth.AuthenticationChannelHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.Timer;

import net.opentsdb.core.TSDB;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 * NOTE: On creation (as of 2.3) the property given in the config for 
 * "tsd.core.connections.limit" will be used to limit the number of concurrent
 * connections supported by the pipeline. The default is zero.
 */
public final class PipelineFactory implements ChannelPipelineFactory {

  // Those are entirely stateless and thus a single instance is needed.
  private static final StringEncoder ENCODER = new StringEncoder();
  private static final WordSplitter DECODER = new WordSplitter();

  // Those are sharable but maintain some state, so a single instance per
  // PipelineFactory is needed.
  //管理当前的连接数
  private final ConnectionManager connmgr;
  //tsdb支持多种网络协议，如http,Telnet等，根据当前channel上传递的数据内容判定具体的使用协议，并在对应的ChannelPipelin中添加处理器
  private final DetectHttpOrRpc HTTP_OR_RPC = new DetectHttpOrRpc();
  //netty中的定时接口
  private final Timer timer;
  //IdleStateHandler类型，netty提供用于检测channl空闲
  private final ChannelHandler timeoutHandler;

  /** Stateless handler for RPCs. */
  //tsdb网络请求核心
  private final RpcHandler rpchandler;
  
  /** The TSDB to which we belong */ 
  private final TSDB tsdb;
  
  /** The server side socket timeout. **/
  //服务端socket连接的超时时间
  private final int socketTimeout;
  
  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins. This constructor creates its own {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @throws RuntimeException if there is an issue loading plugins
   * @throws RuntimeException if the HttpQuery handler is unable to load 
   * serializers
   */
  public PipelineFactory(final TSDB tsdb) {
    this(tsdb, RpcManager.instance(tsdb), 
        tsdb.getConfig().getInt("tsd.core.connections.limit"));
  }

  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins using an already-configured {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @throws RuntimeException if there is an issue loading plugins
   * throws Exception if the HttpQuery handler is unable to load serializers
   */
  public PipelineFactory(final TSDB tsdb, final RpcManager manager) {
    this(tsdb, RpcManager.instance(tsdb), 
        tsdb.getConfig().getInt("tsd.core.connections.limit"));
  }
  
  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins using an already-configured {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @param connections_limit The maximum number of concurrent connections 
   * supported by the TSD.
   * @throws RuntimeException if there is an issue loading plugins
   * throws Exception if the HttpQuery handler is unable to load serializers
   * @since 2.3
   */
  public PipelineFactory(final TSDB tsdb, final RpcManager manager, 
      final int connections_limit) {
    this.tsdb = tsdb;
    socketTimeout = tsdb.getConfig().getInt("tsd.core.socket.timeout");
    timer = tsdb.getTimer();
    //设置空闲读写检测
    timeoutHandler = new IdleStateHandler(timer, 0, 0, socketTimeout);
    rpchandler = new RpcHandler(tsdb, manager);
    connmgr = new ConnectionManager(connections_limit);
    try {
      HttpQuery.initializeSerializerMaps(tsdb);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize formatter plugins", e);
    }
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
   final ChannelPipeline pipeline = pipeline();

    pipeline.addLast("connmgr", connmgr);
    pipeline.addLast("detect", HTTP_OR_RPC);
    return pipeline;
  }

  /**
   * Dynamically changes the {@link ChannelPipeline} based on the request.
   * If a request uses HTTP, then this changes the pipeline to process HTTP.
   * Otherwise, the pipeline is changed to processes an RPC.
   * 判断协议，解析数据
   */
  final class DetectHttpOrRpc extends FrameDecoder {

    @Override
    protected Object decode(final ChannelHandlerContext ctx,
                            final Channel chan,
                            final ChannelBuffer buffer) throws Exception {
      if (buffer.readableBytes() < 1) {  // Yes sometimes we can be called
        return null;                     // with an empty buffer...
      }
      //读取第一个字节
      final int firstbyte = buffer.getUnsignedByte(buffer.readerIndex());
      //获取当前channel关联的ChannelPipeline
      final ChannelPipeline pipeline = ctx.getPipeline();
      // None of the commands in the RPC protocol start with a capital ASCII
      // letter for the time being, and all HTTP commands do (GET, POST, etc.)
      // so use this as a cheap way to differentiate the two.
      //检测第一个字母的范围，如果在A-Z直接，属于http
      if ('A' <= firstbyte && firstbyte <= 'Z') {
        //http编解码处理
        pipeline.addLast("decoder", new HttpRequestDecoder());
        //判断是否支持http chunk
        if (tsdb.getConfig().enable_chunked_requests()) {
          pipeline.addLast("aggregator", new HttpChunkAggregator(
              tsdb.getConfig().max_chunked_requests()));
        }
        // allow client to encode the payload (ie : with gziped json)
        //对Gzip压缩的http请求进行解压处理
        pipeline.addLast("inflater", new HttpContentDecompressor());
        //处理tsdb实例返回的http响应
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
      } else {
        //处理其他协议的解析
        pipeline.addLast("framer", new LineBasedFrameDecoder(1024));
        pipeline.addLast("encoder", ENCODER);
        pipeline.addLast("decoder", DECODER);
      }

      if (tsdb.getAuth() != null) {
        pipeline.addLast("authentication", new AuthenticationChannelHandler(tsdb));
      }
      //添加空闲检测
      pipeline.addLast("timeout", timeoutHandler);
      pipeline.remove(this);
      //添加处理客户端请求逻辑
      pipeline.addLast("handler", rpchandler);

      // Forward the buffer to the next handler.
      return buffer.readBytes(buffer.readableBytes());
    }

  }
  
}
