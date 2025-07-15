package su.rumishistem.rumidataserver;

import java.io.IOException;
import java.sql.SQLException;

import su.rumishistem.rumi_java_lib.SnowFlake;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;

import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;

public class RDS {
	public static void Main(HTTP_EVENT REQ, String PATH) throws IOException, SQLException {
		CheckPATH CP = new CheckPATH(PATH.replaceFirst("\\/rds\\/", ""));

		switch (REQ.getMethod()) {
			case "GET": {
				GET(REQ, CP);
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
			}
		}
	}

	private static void GET(HTTP_EVENT REQ, CheckPATH CP) throws IOException {
		FILER F = new FILER(CP.GetID());
		if (F.exists()) {
			REQ.REPLY_BYTE(200, F.Read());
		} else {
			REQ.REPLY_String(404, "");
		}
	}

	private static void POST(HTTP_EVENT REQ, CheckPATH CP) throws IOException, SQLException {
		boolean PUBLIC = false;

		//モード有るよな？
		if (REQ.getURI_PARAM().get("MODE") == null) {
			REQ.REPLY_String(400, "");
			return;
		}

		if (REQ.getURI_PARAM().get("PUBLIC") != null) {
			if (REQ.getURI_PARAM().get("PUBLIC").equals("true")) {
				//公開にする
				PUBLIC = true;
			}
		}

		if (REQ.getPOST_DATA_BIN().length != 0) {
			switch (REQ.getURI_PARAM().get("MODE")) {
				case "CREATE": {
					if (CP.GetID() == null) {
						//新規作成
						FILER FILE = new FILER(String.valueOf(SnowFlake.GEN()));
						FILE.Create(CP.GetBUCKET(), CP.GetNAME(), PUBLIC);
						FILE.Write(REQ.getPOST_DATA_BIN(), false);
					} else {
						//上書き
						FILER FILE = new FILER(CP.GetID());
						FILE.Write(REQ.getPOST_DATA_BIN(), false);
					}
					break;
				}

				case "APPEND": {
					FILER FILE = new FILER(String.valueOf(SnowFlake.GEN()));
					if (FILE.exists()) {
						FILE.Write(REQ.getPOST_DATA_BIN(), true);
					}
					break;
				}
	
				default: {
					REQ.REPLY_String(400, "");
					return;
				}
			}
		}

		REQ.REPLY_String(200, "");
	}

	private static void DELETE(HTTP_EVENT REQ, CheckPATH CP) throws IOException, SQLException {
		if (CP.GetID() != null) {
			new FILER(CP.GetID()).Remove();
		}

		REQ.REPLY_String(200, "OK");
	}
}
