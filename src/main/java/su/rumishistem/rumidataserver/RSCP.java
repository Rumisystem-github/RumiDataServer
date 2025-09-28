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
import java.sql.SQLException;
import java.util.UUID;
import kotlin.text.Charsets;
import su.rumishistem.rumi_java_lib.SnowFlake;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;
import su.rumishistem.rumi_java_lib.Socket.Server.SocketServer;
import su.rumishistem.rumi_java_lib.Socket.Server.CONNECT_EVENT.CONNECT_EVENT;
import su.rumishistem.rumi_java_lib.Socket.Server.CONNECT_EVENT.CONNECT_EVENT_LISTENER;
import su.rumishistem.rumi_java_lib.Socket.Server.EVENT.CloseEvent;
import su.rumishistem.rumi_java_lib.Socket.Server.EVENT.EVENT_LISTENER;
import su.rumishistem.rumi_java_lib.Socket.Server.EVENT.MessageEvent;
import su.rumishistem.rumi_java_lib.Socket.Server.EVENT.ReceiveEvent;
import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;

public class RSCP {
																					//↓バージョン情報(長さ, バージョン情報)
	private static final byte[] WELCOME_MESSAGE = new byte[] {'R', 'S', 'C', 'P', 0x03, '0', '.', '1'};

	public static void start() throws InterruptedException {
		SocketServer server = new SocketServer();

		server.setEventListener(new CONNECT_EVENT_LISTENER() {
			@Override
			public void CONNECT(CONNECT_EVENT session) throws IOException {
				session.sendData(WELCOME_MESSAGE);

				session.setEventListener(new EVENT_LISTENER() {
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

					@Override
					public void Receive(ReceiveEvent e) {
						byte[] input = e.getByte();
						try {
							if (upload == false && check == false) {
								//コマンド
								ByteArrayInputStream in = new ByteArrayInputStream(input);

								switch (in.read()) {
									//NOOP
									case 0x00:
										session.sendData(new byte[] {0x20});
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
										session.sendData(new byte[] {0x20});
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
											session.sendData(new byte[] {0x20});
										} catch (SQLException ex) {
											ex.printStackTrace();
											session.sendData(new byte[] {0x50});
										}
										return;

									default:
										session.sendData(new byte[] {0x40});
										return;
								}
							} else if (check) {
								//チェック
								ByteArrayInputStream in = new ByteArrayInputStream(input);
								byte[] md5_s = in.readNBytes(16);
								byte[] md5_d = md.digest();

								check = false;
								upload = false;

								//チェックサム
								for (int i = 0; i < md5_s.length; i++) {
									if (md5_s[i] != md5_d[i]) {
										LOG(LOG_TYPE.FAILED, "チェックサム受信、不整合があります。");
										session.sendData(new byte[] {0x40});
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
									} else {
										FILER filer = new FILER(cp.GetID());
										filer.write_from_file(file);
									}
								} catch (SQLException ex) {
									ex.printStackTrace();
									session.sendData(new byte[] {0x50});
								}

								//後処理
								file.delete();

								session.sendData(new byte[] {0x20});
							} else if (upload) {
								//転送中
								receive_size += input.length;
								LOG(LOG_TYPE.INFO, "RSCP データ受信:" + input.length + "バイト " + receive_size + "\\" + file_size + "バイト");

								md.update(input);
								fos.write(input);
								fos.flush();

								if (file_size < (receive_size + 1)) {
									fos.close();
									check = true;

									LOG(LOG_TYPE.OK, "全てを受信した");
								}
							}
						} catch (IOException ex) {
							ex.printStackTrace();
						}
					}

					@Override
					public void Message(MessageEvent e) {}

					@Override
					public void Close(CloseEvent e) {
						System.out.println("クローズ");
					}
				});
			}
		});

		server.START(41029);
	}
}
