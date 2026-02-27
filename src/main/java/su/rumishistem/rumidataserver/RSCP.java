package su.rumishistem.rumidataserver;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import io.netty.channel.socket.SocketChannel;
import java.sql.SQLException;
import java.util.UUID;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import kotlin.text.Charsets;
import su.rumishistem.rumi_java_lib.SnowFlake;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;
import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;

public class RSCP {
																					//↓バージョン情報(長さ, バージョン情報)
	private static final byte[] WELCOME_MESSAGE = new byte[] {'R', 'S', 'C', 'P', 0x03, '0', '.', '1'};

	public static void start() throws InterruptedException {
		EventLoopGroup parent_group = new NioEventLoopGroup(1);
		EventLoopGroup worker_group = new NioEventLoopGroup();

		ServerBootstrap b = new ServerBootstrap();
		b.group(parent_group, worker_group);
		b.channel(NioServerSocketChannel.class);
		b.childHandler(new ChannelInitializer<SocketChannel>() {
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
					private File file;
					private FileOutputStream fos;
					private String bucket;
					private String file_name;
					private boolean is_public = false;
					private long file_size = 0;

					private boolean upload = false;
					private boolean check = false;
					private long receive_size = 0;
					private MessageDigest md;

					//接続
					@Override
					public void channelActive(ChannelHandlerContext ctx) throws Exception {
						LOG(LOG_TYPE.INFO, "RSCP New Session");

						send(ctx, WELCOME_MESSAGE);
					}

					//切断
					@Override
					public void channelInactive(ChannelHandlerContext ctx) throws Exception {
						LOG(LOG_TYPE.INFO, "RSCP Close Session");
					}

					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
						ByteBuf buf = (ByteBuf)msg;
						byte[] input = new byte[buf.readableBytes()];
						buf.readBytes(input);

						try {
							if (upload == false && check == false) {
								//コマンド
								ByteArrayInputStream in = new ByteArrayInputStream(input);

								switch (in.read()) {
									//NOOP
									case 0x00:
										send(ctx, new byte[] {0x20});
										return;

									//UPLOAD
									case 0x01:
										upload = true;
										bucket = new String(in.readNBytes(in.read()), Charsets.UTF_8);
										file_name = new String(in.readNBytes(in.read()), Charsets.UTF_8);
										if (in.read() == 1) is_public = true;
										ByteBuffer bb = ByteBuffer.wrap(in.readNBytes(8));
										bb.order(ByteOrder.BIG_ENDIAN);
										file_size = bb.getLong();

										//ファイルを準備
										file = new File("/tmp/" + UUID.randomUUID().toString());
										file.createNewFile();
										fos = new FileOutputStream(file);

										//チェックサム用
										try {
											md = MessageDigest.getInstance("MD5");
										} catch (NoSuchAlgorithmException ex) {
											//起こるわけねーだろ
										}
	
										LOG(LOG_TYPE.OK, "RSCP バケット[" + bucket + "]にファイル作成:" + file_name);
										send(ctx, new byte[] {0x10});
										return;

									//削除
									case 0x02:
										bucket = new String(in.readNBytes(in.read()), Charsets.UTF_8);
										file_name = new String(in.readNBytes(in.read()), Charsets.UTF_8);

										try {
											CheckPATH cp = new CheckPATH(bucket + "/" + file_name);
											if (cp.GetID() != null) {
												new FILER(cp.GetID()).Remove();
											}

											LOG(LOG_TYPE.OK, "RSCP バケット[" + bucket + "]の[" + file_name + "]を削除");
											send(ctx, new byte[] {0x20});
										} catch (SQLException ex) {
											ex.printStackTrace();
											send(ctx, new byte[] {0x50});
										}
										return;

									default:
										send(ctx, new byte[] {0x40});
										return;
								}
							} else if (check) {
								//チェック
								ByteArrayInputStream in = new ByteArrayInputStream(input);
								byte[] md5_s = in.readNBytes(16);
								byte[] md5_d = md.digest();

								check = false;
								upload = false;

								handle_checksum(ctx, md5_s, md5_d);
							} else if (upload) {
								//転送中
								long remain = file_size - receive_size;

								//LOG(LOG_TYPE.INFO, "RSCP データ受信:" + input.length + "バイト " + receive_size + "\\" + file_size + "バイト");

								if (input.length >= remain) {
									md.update(input, 0, (int)remain);
									fos.write(input, 0, (int)remain);
									receive_size += remain;
									fos.close();

									LOG(LOG_TYPE.OK, "全てを受信した");

									check = true;
									upload = false;

									//もし余ったならそれはMD5
									int extra = input.length - (int)remain;
									if (extra > 0) {
										byte[] md5_part = new byte[extra];
										System.arraycopy(input, (int)remain, md5_part, 0, extra);
										handle_checksum(ctx, md5_part, md.digest());
									} else {
										fos.close();
										check = true;
										send(ctx, new byte[] {0x10});
									}
									return;
								}

								md.update(input);
								fos.write(input);
								receive_size += input.length;
							}
						} catch (IOException ex) {
							ex.printStackTrace();
						} finally {
							buf.release();
						}
					}

					private void handle_checksum(ChannelHandlerContext ctx, byte[] md5_receive, byte[] md5_saved) throws IOException {
						//チェックサム
						for (int i = 0; i < md5_receive.length; i++) {
							if (md5_receive[i] != md5_saved[i]) {
								LOG(LOG_TYPE.FAILED, "チェックサム受信、不整合があります。");
								send(ctx, new byte[] {0x40});
								return;
							}
						}
						LOG(LOG_TYPE.OK, "チェックサム受信、チェックしました");
					
						//アップロード
						try {
							CheckPATH cp = new CheckPATH(bucket + "/" + file_name);
							if (cp.GetID() == null) {
								//新規作成
								FILER filer = new FILER(String.valueOf(SnowFlake.GEN()));
								filer.Create(bucket, file_name, is_public);
								filer.write_from_file(file);
							} else {
								FILER filer = new FILER(cp.GetID());
								filer.write_from_file(file);
							}
						} catch (SQLException ex) {
							ex.printStackTrace();
							send(ctx, new byte[] {0x50});
						}
					
						//後処理
						file.delete();
					
						send(ctx, new byte[] {0x20});
					}
				});
			};
		});
		b.option(ChannelOption.SO_BACKLOG, 128);
		b.childOption(ChannelOption.SO_KEEPALIVE, true);


		ChannelFuture f = b.bind(41029).sync();
		System.out.println("RSCP Start 41029");
		f.channel().closeFuture().sync();
	}

	private static void send(ChannelHandlerContext ctx, byte[] data) {
		ctx.writeAndFlush(ctx.alloc().buffer().writeBytes(data));
	}
}
