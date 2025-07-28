package su.rumishistem.rumidataserver;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

import su.rumishistem.rumi_java_lib.HASH;
import su.rumishistem.rumi_java_lib.HASH.HASH_TYPE;
import su.rumishistem.rumi_java_lib.SnowFlake;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;
import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;

public class S3 {
	private static HashMap<String, HashMap<String, Object>> MULTIPART_QUEUE = new HashMap<String, HashMap<String, Object>>();

	public static void Main(HTTP_EVENT REQ, String PATH) throws IOException, SQLException, NoSuchAlgorithmException {
		CheckPATH CP = new CheckPATH(PATH.replaceFirst("\\/s3\\/", ""));

		switch (REQ.getMethod()) {
			case "GET": {
				GET(REQ, CP);
				return;
			}

			case "PUT": {
				PUT(REQ, CP);
				return;
			}

			case "POST": {
				POST(REQ, CP);
				return;
			}

			case "DELETE": {
				DELETE(REQ, CP);
				return;
			}

			default: {
				REQ.REPLY_String(501, "");
				return;
			}
		}
	}

	private static void GET(HTTP_EVENT REQ, CheckPATH CP) throws IOException {
		FILER F = new FILER(CP.GetID());
		if (F.exists()) {
			F.Read(REQ.getCTX());
		} else {
			REQ.REPLY_String(404, "");
		}
	}

	private static void POST(HTTP_EVENT REQ, CheckPATH CP) throws IOException, SQLException, NoSuchAlgorithmException {
		//マルチパートアップロード
		if (REQ.getURI_PARAM().get("uploadId") == null) {
			//初期化
			String QUEUE_ID = String.valueOf(SnowFlake.GEN());
			HashMap<String, Object> QUEUE = new HashMap<String, Object>();
			QUEUE.put("ID", QUEUE_ID);
			QUEUE.put("BUCKET", CP.GetBUCKET());
			QUEUE.put("NAME", CP.GetNAME());
			QUEUE.put("PUBLIC", false);

			//公開設定
			if (REQ.getHEADER_DATA().get("X-AMZ-ACL") != null) {
				if (REQ.getHEADER_DATA().get("X-AMZ-ACL").equals("public-read")) {
					QUEUE.put("PUBLIC", true);
				}
			}

			//一時ファイル作成
			Files.createFile(Path.of("/tmp/RDS-" + CP.GetBUCKET() + "-" + QUEUE_ID));

			//キューに追加
			MULTIPART_QUEUE.put(QUEUE_ID, QUEUE);

			//返答
			REQ.REPLY_String(200,
				"<InitiateMultipartUploadResult>"+
					"<UploadId>" + QUEUE_ID + "</UploadId>"+
				"</InitiateMultipartUploadResult>"
			);

			LOG(LOG_TYPE.INFO, "Initialize Multipart upload:" + QUEUE_ID);
			return;
		} else {
			//マルチパートアップロード完了
			String QUEUE_ID = REQ.getURI_PARAM().get("uploadId");
			if (MULTIPART_QUEUE.get(QUEUE_ID) != null) {
				HashMap<String, Object> QUEUE = MULTIPART_QUEUE.get(QUEUE_ID);
				String ID = (String) QUEUE.get("ID");
				String BUCKET = (String) QUEUE.get("BUCKET");
				String NAME = (String) QUEUE.get("NAME");
				boolean PUBLIC = (boolean) QUEUE.get("PUBLIC");

				//結合用のファイルを作成
				File JoinTempFile = new File("/tmp/RDS-" + UUID.randomUUID().toString());
				FileOutputStream FOS = new FileOutputStream(JoinTempFile, true);

				for (int I = 1; I < 10000; I++) {
					File TempFile = new File("/tmp/RDS-" + CP.GetBUCKET() + "-" + QUEUE_ID + "-" + I);
					if (!TempFile.exists()) break;

					FileInputStream FIS = new FileInputStream(TempFile);
					byte[] Buffer = new byte[4096];
					int ByteRead;

					while ((ByteRead = FIS.read(Buffer)) != -1) {
						FOS.write(Buffer, 0, ByteRead);
						FOS.flush();
					}

					FIS.close();
					TempFile.delete();
				}

				FOS.close();

				//ファイルを登録して書き込む
				FILER FILE = new FILER(ID);
				FILE.Create(BUCKET, NAME, PUBLIC);
				FILE.Write(Files.readAllBytes(JoinTempFile.toPath()), false);
				//TODO:↑阿呆みたいなファイル入れられたらメモリがギャーってなるので治す

				JoinTempFile.delete();

				//Etag、ようはハッシュ、これはファイル全体
				//REQ.setHEADER("ETag", HASH.Gen(HASH_TYPE.MD5, FILE.Read()));
				REQ.REPLY_BYTE(200, new byte[] {});

				LOG(LOG_TYPE.INFO, "Multipart upload DONE!!!!!!!!!!!!:" + QUEUE_ID);
			} else {
				//キューが無いので蹴る
				REQ.REPLY_String(404, "Fuck you");
				LOG(LOG_TYPE.INFO, "Queue ga nai:" + QUEUE_ID);
				return;
			}
		}
	}

	private static void PUT(HTTP_EVENT REQ, CheckPATH CP) throws IOException, SQLException, NoSuchAlgorithmException {
		if (REQ.getURI_PARAM().get("uploadId") == null) {
			//単一アップロード
			boolean PUBLIC = false;

			//公開設定
			if (REQ.getHEADER_DATA().get("X-AMZ-ACL") != null) {
				if (REQ.getHEADER_DATA().get("X-AMZ-ACL").equals("public-read")) {
					PUBLIC = true;
				}
			}

			FILER FILE = new FILER(String.valueOf(SnowFlake.GEN()));
			FILE.Create(CP.GetBUCKET(), CP.GetNAME(), PUBLIC);
			FILE.Write(REQ.getPOST_DATA_BIN(), false);

			REQ.REPLY_String(200, "");
			return;
		} else {
			//マルチパートアップロード
			String QUEUE_ID = REQ.getURI_PARAM().get("uploadId");
			String PartNumber = REQ.getURI_PARAM().get("partNumber");
			if (MULTIPART_QUEUE.get(QUEUE_ID) != null) {
				File TEMP_FILE = new File("/tmp/RDS-" + CP.GetBUCKET() + "-" + QUEUE_ID + "-" + PartNumber);
				TEMP_FILE.createNewFile();

				//一時ファイルに追記
				FileOutputStream FOS = new FileOutputStream(TEMP_FILE, true);
				FOS.write(REQ.getPOST_DATA_BIN());
				FOS.close();

				LOG(LOG_TYPE.OK, "Temp file write:" + QUEUE_ID + "/" + PartNumber);

				//ETag、ようはハッシュ、これはパートごと
				REQ.setHEADER("ETag", HASH.Gen(HASH_TYPE.MD5, REQ.getPOST_DATA_BIN()));
				REQ.REPLY_String(200, "");
				return;
			} else {
				//キューが無いので蹴る
				REQ.REPLY_String(404, "Fuck you");
				LOG(LOG_TYPE.INFO, "Queue ga nai:" + QUEUE_ID);
				return;
			}
		}
	}

	private static void DELETE(HTTP_EVENT REQ, CheckPATH CP) throws IOException, SQLException {
		if (CP.GetID() != null) {
			new FILER(CP.GetID()).Remove();
		}

		REQ.REPLY_String(204, "");
		return;
	}
}
