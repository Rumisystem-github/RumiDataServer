package com.rumisystem.rumidataserver.MODULE;

import static com.rumisystem.rumidataserver.Main.CONFIG_DATA;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.rumisystem.rumi_java_lib.ArrayNode;
import com.rumisystem.rumi_java_lib.SQL;

public class FILER {
	private String ID = null;
	private String FILE_PATH = null;

	public FILER(String ID) {
		this.ID = ID;
		this.FILE_PATH = CONFIG_DATA.get("DIR").asString("PATH") + ID;
	}

	public boolean exists() {
		return Files.exists(Path.of(FILE_PATH));
	}

	public byte[] Read() throws IOException {
		return Files.readAllBytes(Path.of(FILE_PATH));
	}

	public void Write(byte[] DATA, boolean APPEND) throws IOException {
		if (!Files.exists(Path.of(FILE_PATH))) {
			//ファイルがないので作成
			Files.createFile(Path.of(FILE_PATH));
		}

		//書き込み
		FileOutputStream FOS = new FileOutputStream(new File(FILE_PATH), APPEND);
		FOS.write(DATA);
		FOS.close();
	}

	public boolean isPublic() {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `ID` = ?;", new Object[] {ID});
		if (SQL_RESULT.asArrayList().size() == 1) {
			return SQL_RESULT.get(0).asBool("PUBLIC");
		} else {
			return false;
		}
	}
}
