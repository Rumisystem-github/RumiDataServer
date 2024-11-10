package com.rumisystem.rumidataserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import com.rumisystem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;

public class S3 {
	public static void Main(HTTP_EVENT REQ) throws IOException, SQLException {
		String BUCKET = REQ.getEXCHANGE().getRequestURI().getPath().toString().replace("/s3/", "").split("/")[0];
		String NAME = REQ.getEXCHANGE().getRequestURI().getPath().toString().replace("/s3/" + BUCKET + "/", "");

		switch (REQ.getEXCHANGE().getRequestMethod()) {
			//ファイル追加
			case "PUT": {
				PUT(REQ, BUCKET, NAME);
				break;
			}

			//ファイル削除
			case "DELETE": {
				DELETE(REQ, BUCKET, NAME);
				break;
			}

			default: {
				REQ.REPLY_String(400, "400");
			}
		}
	}

	private static void PUT(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		if (REQ.getPOST_DATA_BIN().length != 0) {
			FILER.CreateFile(BUCKET, NAME, REQ.getPOST_DATA_BIN());
		}

		REQ.REPLY_String(200, "");
	}

	private static void DELETE(HTTP_EVENT REQ, String BUCKET, String NAME) throws IOException, SQLException {
		FILER.DeleteFile(BUCKET, NAME);

		REQ.REPLY_String(200, "");
	}
}
