package su.rumishistem.rumidataserver.MODULE;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;

public class FileLoader {
	public static Map<String, byte[]> cache = Collections.synchronizedMap(new LRUCache(100));

	public static void read(String file_path, ChannelHandlerContext ctx, String id, String file_id) throws IOException {
		File f = new File(file_path);

		DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
		ctx.write(response);

		//ファイルサイズが100MB以下ならキャッシュを
		if (f.length() <= 100 * 1024 * 1024) {
			byte[] data = null;
			if (cache.get(file_path) != null) {
				//キャッシュからロード
				data = cache.get(file_path);

				LOG(LOG_TYPE.OK, "Cache Read ID:" + id + " FileID:" + file_id);
			} else {
				data = Files.readAllBytes(Path.of(file_path));
				cache.put(file_path, data);

				LOG(LOG_TYPE.OK, "Cache Load ID:" + id + " FileID:" + file_id);
			}

			ByteBuf chunk_data = Unpooled.copiedBuffer(data, 0, data.length);
			HttpContent chunk = new DefaultHttpContent(chunk_data);
			ctx.writeAndFlush(chunk);
		} else {
			//デカイのでキャッシュは使わずに読み込み
			FileInputStream fis = new FileInputStream(f);
			byte[] buffer = new byte[4096];
			int length;
			while ((length = fis.read(buffer)) != -1) {
				ByteBuf chunk_data = Unpooled.copiedBuffer(buffer, 0, length);
				HttpContent chunk = new DefaultHttpContent(chunk_data);
				ctx.writeAndFlush(chunk);
			}
			fis.close();

			LOG(LOG_TYPE.OK, "Load ID:" + id + " FileID:" + file_id);
		}

		ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(ChannelFutureListener.CLOSE);
	}
}
