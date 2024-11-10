package com.rumisystem.rumidataserver;

import static com.rumisystem.rumidataserver.Main.CONFIG_DATA;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

import com.rumisystem.rumi_java_lib.ArrayNode;
import com.rumisystem.rumi_java_lib.SQL;
import com.rumisystem.rumi_java_lib.SnowFlake;

public class FILER {
	public static boolean CreateFile(String BUCKET, String NAME, byte[] DATA) throws IOException, SQLException {
		if (GetID(BUCKET, NAME) == null) {
			//ファイル新規作成
			Long ID = SnowFlake.GEN();
			String FILE_PATH = CONFIG_DATA.get("DIR").asString("PATH") + ID;

			SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`) VALUES (?, ?, ?);", new Object[] {
				ID,
				BUCKET,
				NAME
			});

			//ファイル作成
			new File(FILE_PATH).createNewFile();

			//書き込み
			FileOutputStream FOS = new FileOutputStream(Path.of(FILE_PATH).toFile());
			FOS.write(DATA);
			FOS.close();

			return true;
		} else {
			//ファイル上書き
			String ID = GetID(BUCKET, NAME);
			String FILE_PATH = CONFIG_DATA.get("DIR").asString("PATH") + ID;

			//書き込み
			FileOutputStream FOS = new FileOutputStream(Path.of(FILE_PATH).toFile());
			FOS.write(DATA);
			FOS.close();
			return true;
		}
	}

	public static byte[] OpenFile(String BUCKET, String NAME) throws IOException {
		File FILE = new File(CONFIG_DATA.get("DIR").asString("PATH") + GetID(BUCKET, NAME));
		//ファイルが存在するか
		if (FILE.exists()) {
			byte[] FILE_DATA = new byte[(int) FILE.length()];
			FileInputStream FIS = new FileInputStream(FILE);
			int BYTES_READ = FIS.read(FILE_DATA);
			FIS.close();
			if (BYTES_READ == FILE_DATA.length) {
				return FILE_DATA;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public static void DeleteFile(String BUCKET, String NAME) throws SQLException {
		String ID = GetID(BUCKET, NAME);
		SQL.UP_RUN("DELETE FROM `DATA` WHERE `DATA`.`ID` = ?;", new Object[] {ID});

		File FILE = new File(CONFIG_DATA.get("DIR").asString("PATH") + ID);
		//ファイルが存在するか
		if (FILE.exists()) {
			FILE.delete();
		}
	}
	
	private static String GetID(String BUCKET, String NAME) {
		ArrayNode RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `DATA`.`BUCKET` = BINARY ? AND `DATA`.`NAME` = BINARY ?;", new Object[] {
			BUCKET,
			NAME
		});

		if (RESULT.asArrayList().size() == 1) {
			return RESULT.get(0).asString("ID");
		} else {
			return null;
		}
	}
}
