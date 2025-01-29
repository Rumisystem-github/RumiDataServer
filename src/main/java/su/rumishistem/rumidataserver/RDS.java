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

		switch (REQ.getEXCHANGE().getRequestMethod()) {
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
				REQ.REPLY_String(400, "");
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
		if (REQ.getURI_PARAM().get("MODE") == null) {
			REQ.REPLY_String(400, "");
			return;
		}

		if (REQ.getPOST_DATA_BIN().length != 0) {
			switch (REQ.getURI_PARAM().get("MODE")) {
				case "CREATE": {
					new FILER(String.valueOf(SnowFlake.GEN())).Write(CP.GetBUCKET(), CP.GetNAME(), REQ.getPOST_DATA_BIN(), false);
					break;
				}
				
				case "APPEND": {
					new FILER(String.valueOf(SnowFlake.GEN())).Write(CP.GetBUCKET(), CP.GetNAME(), REQ.getPOST_DATA_BIN(), true);
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
