package com.rumisystem.rumidataserver.MODULE;

import com.rumisystem.rumi_java_lib.ArrayNode;
import com.rumisystem.rumi_java_lib.SQL;

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
			return SQL_RESULT.get(0).asString("ID");
		} else {
			return null;
		}
	}
}
