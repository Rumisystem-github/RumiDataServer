package com.rumisystem.rumidataserver;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;

import com.rumisystem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;

public class RDS {
	public static void Main(HTTP_EVENT REQ) throws IOException, SQLException, NoSuchAlgorithmException {
		String[] URI = REQ.getEXCHANGE().getRequestURI().getPath().toString().replace("/rds/", "").split("/");
		String BUCKET = URI[0];
		String NAME = String.join("/", Arrays.copyOfRange(URI, 1, URI.length));

		switch (REQ.getEXCHANGE().getRequestMethod()) {
			//ファイル追加
			case "POST": {
				POST(REQ, BUCKET, NAME);
				break;
			}

			//ファイル削除
			case "DELETE": {
				DELETE(REQ, BUCKET, NAME);
				break;
			}

			//ファイル取得
			case "GET": {
				GET(REQ, BUCKET, NAME);
				break;
			}

			default: {
				REQ.REPLY_String(400, "400");
			}
		}
	}

	private static void POST(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException, NoSuchAlgorithmException {
		if (REQ.getURI_PARAM().get("MODE") == null) {
			REQ.REPLY_String(400, "");
			return;
		}

		if (REQ.getPOST_DATA_BIN().length != 0) {
			if (REQ.getURI_PARAM().get("MODE").equals("CREATE")) {
				FILER.CreateFile(BUCKET, NAME, REQ.getPOST_DATA_BIN());
				FILER.FileClose(BUCKET, NAME);
			} else if (REQ.getURI_PARAM().get("MODE").equals("APPEND")) {
				FILER.AppendFile(BUCKET, NAME, REQ.getPOST_DATA_BIN());
			} else if (REQ.getURI_PARAM().get("MODE").equals("CLOSE")) {
				FILER.FileClose(BUCKET, NAME);
			}
		}

		REQ.REPLY_String(200, "");
	}

	private static void DELETE(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		FILER.DeleteFile(BUCKET, NAME);

		REQ.REPLY_String(200, "");
	}

	private static void GET(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		// ファイルのデータを返す
		byte[] DATA = FILER.OpenFile(BUCKET, NAME);
		if (DATA != null) {
			REQ.REPLY_BYTE(200, DATA);
		} else {
			REQ.REPLY_String(404, "404");
		}
	}
}
