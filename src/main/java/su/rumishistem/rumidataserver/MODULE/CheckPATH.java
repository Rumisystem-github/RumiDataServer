package su.rumishistem.rumidataserver.MODULE;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;

import su.rumishistem.rumi_java_lib.ArrayNode;
import su.rumishistem.rumi_java_lib.SQL;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;

public class CheckPATH {
	private String BUCKET = null;
	private String NAME = null;

	public CheckPATH(String PATH) {
		//「/」から始まるならそれを消す
		if (PATH.startsWith("/")) {
			PATH = PATH.replaceFirst("\\/", "");
		}

		//バケット名
		BUCKET = PATH.split("\\/")[0];

		//ファイル名
		NAME = PATH.replaceFirst(BUCKET, "");
		//ファイル名が「/」から始まるなら消す
		if (NAME.startsWith("/")) {
			NAME = NAME.replaceFirst("\\/", "");
		}

		LOG(LOG_TYPE.OK, "Check BUCKET:" + BUCKET + " NAME:" + NAME);
	}

	public String GetBUCKET() {
		return BUCKET;
	}

	public String GetNAME() {
		return NAME;
	}

	public String GetID() {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `BUCKET` = ? AND `NAME` = ?", new Object[] {BUCKET, NAME});
		if (SQL_RESULT.asArrayList().size() == 1) {
			return SQL_RESULT.get(0).getData("ID").asString();
		} else {
			return null;
		}
	}

	public String GetFileID() {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `BUCKET` = ? AND `NAME` = ?", new Object[] {BUCKET, NAME});
		if (SQL_RESULT.asArrayList().size() == 1) {
			return SQL_RESULT.get(0).getData("FILE").asString();
		} else {
			return null;
		}
	}
}
