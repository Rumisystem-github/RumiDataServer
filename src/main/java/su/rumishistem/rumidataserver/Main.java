package su.rumishistem.rumidataserver;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import su.rumishistem.rumi_java_lib.ArrayNode;
import su.rumishistem.rumi_java_lib.CONFIG;
import su.rumishistem.rumi_java_lib.SQL;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT_LISTENER;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_SERVER;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;

import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;
import su.rumishistem.rumidataserver.MODULE.LRUCache;

public class Main {
	public static ArrayNode CONFIG_DATA = null;
	public static Map<String, byte[]> FC = Collections.synchronizedMap(new LRUCache(100));

	public static void main(String[] args) {
		try {
			//設定ファイルを読み込む
			if (new File("Config.ini").exists()) {
				CONFIG_DATA = new CONFIG().DATA;
				LOG(LOG_TYPE.PROCESS_END_OK, "");
			} else {
				LOG(LOG_TYPE.PROCESS_END_FAILED, "");
				LOG(LOG_TYPE.FAILED, "ERR! Config.ini ga NAI!!!!!!!!!!!!!!");
				System.exit(1);
			}

			//SQL
			SQL.CONNECT(
					CONFIG_DATA.get("SQL").getData("IP").asString(),
					CONFIG_DATA.get("SQL").getData("PORT").asString(),
					CONFIG_DATA.get("SQL").getData("DB").asString(),
					CONFIG_DATA.get("SQL").getData("USER").asString(),
					CONFIG_DATA.get("SQL").getData("PASS").asString()
			);

			HTTP_SERVER SERVER = new HTTP_SERVER(3006);
			SERVER.SetThreadNum(500);
			SERVER.SET_EVENT_VOID(new HTTP_EVENT_LISTENER() {
				@Override
				public void REQUEST_EVENT(HTTP_EVENT REQ) {
					try {
						String PATH = REQ.getURI().getPath();

						if (PATH.startsWith("/rds/")) {
							//RDS
							RDS.Main(REQ, PATH);
							return;
						} else if (PATH.startsWith("/s3/")) {
							//S3
							S3.Main(REQ, PATH);
							return;
						} else if (PATH.equals("/data/CheckStatus")) {
							//ステータスチェック
							return;
						} else if (PATH.startsWith("/data/")) {
							//データを読む
							CheckPATH CP = new CheckPATH(PATH.replaceFirst("\\/data\\/", ""));
							FILER filer = new FILER(CP.GetID());
							if (filer.exists()) {
								if (filer.isPublic()) {
									String path = filer.getFilePath();
									File f = new File(path);

									DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
									response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
									response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
									REQ.getCTX().write(response);

									//4096ずつ読み込み
									FileInputStream fis = new FileInputStream(f);
									byte[] buffer = new byte[4096];
									int length;
									while ((length = fis.read(buffer)) != -1) {
										ByteBuf chunk_data = Unpooled.copiedBuffer(buffer, 0, length);
										HttpContent chunk = new DefaultHttpContent(chunk_data);
										REQ.getCTX().writeAndFlush(chunk);
									}
									fis.close();

									REQ.getCTX().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(ChannelFutureListener.CLOSE);
									return;
								} else {
									REQ.REPLY_String(400, "");
									return;
								}
							} else {
								REQ.REPLY_String(404, "");
								return;
							}
						} else {
							//どれでもない
							REQ.REPLY_String(400, "400");
							return;
						}
					} catch (Exception EX) {
						System.out.println(REQ.getURI());
						EX.printStackTrace();
						try {
							REQ.REPLY_String(200, "");
						} catch (Exception EXX) {
							//ひねりつぶす
						}
					}
				}
			});
			SERVER.setVERBOSE(true);
			SERVER.START_HTTPSERVER();
		} catch (Exception EX) {
			EX.printStackTrace();
		}
	}
}
