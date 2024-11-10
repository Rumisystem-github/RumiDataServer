package com.rumisystem.rumidataserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import com.rumisystem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;

public class RDS {
	public static void Main(HTTP_EVENT REQ) throws IOException, SQLException {
		String BUCKET = REQ.getEXCHANGE().getRequestURI().getPath().toString().replace("/rds/", "").split("/")[0];
		String NAME = REQ.getEXCHANGE().getRequestURI().getPath().toString().replace("/rds/", "").split("/")[1];

		switch (REQ.getEXCHANGE().getRequestMethod()) {
			//ファイル追加
			case "POST": {
				POST(REQ, BUCKET, NAME);
				break;
			}

			//ファイル追記
			case "PATCH": {
				PATCH(REQ, BUCKET, NAME);
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

	private static void POST(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		if (REQ.getPOST_DATA_BIN().length != 0) {
			FILER.CreateFile(BUCKET, NAME, REQ.getPOST_DATA_BIN());
		}

		REQ.REPLY_String(200, "");
	}

	private static void PATCH(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		if (REQ.getPOST_DATA_BIN().length != 0) {
			FILER.AppendFile(BUCKET, NAME, REQ.getPOST_DATA_BIN());
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
