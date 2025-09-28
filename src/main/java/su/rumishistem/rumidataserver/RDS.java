package su.rumishistem.rumidataserver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import su.rumishistem.rumi_java_lib.SnowFlake;
import su.rumishistem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;

import su.rumishistem.rumidataserver.MODULE.CheckPATH;
import su.rumishistem.rumidataserver.MODULE.FILER;

public class RDS {
	public static void Main(HTTP_EVENT REQ, String PATH) throws IOException, SQLException {
		CheckPATH CP = new CheckPATH(PATH.replaceFirst("\\/rds\\/", ""));

		//これは外部に露出するわけではなく、ローカルのサーバー同士での通信に用いるため、認証システムは不要
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
			F.Read(REQ.getCTX());
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
			File temp_file = new File("/tmp/" + UUID.randomUUID().toString());
			FileOutputStream fos = new FileOutputStream(temp_file);
			fos.write(REQ.getPOST_DATA_BIN());
			fos.flush();
			fos.close();

			try {
				switch (REQ.getURI_PARAM().get("MODE")) {
					case "CREATE": {
						if (CP.GetID() == null) {
							//新規作成
							FILER FILE = new FILER(String.valueOf(SnowFlake.GEN()));
							FILE.Create(CP.GetBUCKET(), CP.GetNAME(), PUBLIC);
							FILE.write_from_file(temp_file);
						} else {
							//上書き
							FILER FILE = new FILER(CP.GetID());
							FILE.write_from_file(temp_file);
						}
						break;
					}

					default: {
						REQ.REPLY_String(400, "");
						return;
					}
				}
			} finally {
				temp_file.delete();
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
