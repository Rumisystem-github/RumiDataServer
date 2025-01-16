package com.rumisystem.rumidataserver;

import java.io.IOException;

import com.rumisystem.rumi_java_lib.SnowFlake;
import com.rumisystem.rumi_java_lib.HTTP_SERVER.HTTP_EVENT;
import com.rumisystem.rumidataserver.MODULE.CheckPATH;
import com.rumisystem.rumidataserver.MODULE.FILER;

public class RDS {
	public static void Main(HTTP_EVENT REQ, String PATH) throws IOException {
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

	private static void POST(HTTP_EVENT REQ, CheckPATH CP) throws IOException {
		if (REQ.getURI_PARAM().get("MODE") == null) {
			REQ.REPLY_String(400, "");
			return;
		}

		if (REQ.getPOST_DATA_BIN().length != 0) {
			switch (REQ.getURI_PARAM().get("MODE")) {
				case "CREATE": {
					new FILER(String.valueOf(SnowFlake.GEN())).Write(REQ.getPOST_DATA_BIN(), false);
					break;
				}
				
				case "APPEND": {
					new FILER(String.valueOf(SnowFlake.GEN())).Write(REQ.getPOST_DATA_BIN(), true);
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
}
