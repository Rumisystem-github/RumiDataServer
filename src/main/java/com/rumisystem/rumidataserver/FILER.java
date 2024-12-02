package com.rumisystem.rumidataserver;

import static com.rumisystem.rumidataserver.Main.CONFIG_DATA;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import com.rumisystem.rumi_java_lib.ArrayNode;
import com.rumisystem.rumi_java_lib.HASH;
import com.rumisystem.rumi_java_lib.HASH.HASH_TYPE;
import com.rumisystem.rumi_java_lib.SQL;
import com.rumisystem.rumi_java_lib.SnowFlake;

public class FILER {
	private static String GenTempFileName(String BUCKET, String NAME) {
		NAME = NAME.replace("/", "_-_");

		return CONFIG_DATA.get("DIR").asString("TEMP") + BUCKET + "_" + NAME;
	}

	public static boolean CreateFile(String BUCKET, String NAME, byte[] DATA) throws IOException, SQLException {
		if (GetID(BUCKET, NAME) == null) {
			//ファイル新規作成
			String FILE_PATH = GenTempFileName(BUCKET, NAME);

			//ファイル作成
			new File(FILE_PATH).createNewFile();

			//書き込み
			FileOutputStream FOS = new FileOutputStream(Path.of(FILE_PATH).toFile());
			FOS.write(DATA);
			FOS.close();

			return true;
		} else {
			//ファイル上書き
			String FILE_PATH = GenTempFileName(BUCKET, NAME);

			//書き込み
			FileOutputStream FOS = new FileOutputStream(Path.of(FILE_PATH).toFile());
			FOS.write(DATA);
			FOS.close();
			return true;
		}
	}

	public static boolean AppendFile(String BUCKET, String NAME, byte[] DATA) throws IOException, SQLException {
		if (GetID(BUCKET, NAME) != null) {
			//ファイル追記
			String FILE_PATH = GenTempFileName(BUCKET, NAME);

			//書き込み
			FileOutputStream FOS = new FileOutputStream(Path.of(FILE_PATH).toFile(), true);
			FOS.write(DATA);
			FOS.close();
		}

		return true;
	}

	public static byte[] OpenFile(String BUCKET, String NAME) throws IOException {
		File FILE = new File(CONFIG_DATA.get("DIR").asString("PATH") + GetFILEID(BUCKET, NAME));
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

	public static void FileClose(String BUCKET, String NAME) throws NoSuchAlgorithmException, IOException, SQLException {
		File TEMPFILE = new File(GenTempFileName(BUCKET, NAME));
		//ファイルが存在するか
		if (TEMPFILE.exists()) {
			String HASH_TEXT = HASH.Gen(HASH_TYPE.SHA3_256, Files.readAllBytes(TEMPFILE.toPath()));
			ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `HASH` = BINARY ?;", new Object[] {HASH_TEXT});
			if (SQL_RESULT.asArrayList().size() == 0) {
				//ファイルを保存
				long ID = SnowFlake.GEN();
				long FID = SnowFlake.GEN();

				//SQLに登録
				SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`, `FILE`, `HASH`) VALUES (?, ?, ?, ?, ?)", new Object[] {
					ID,
					BUCKET,
					NAME,
					FID,
					HASH_TEXT
				});

				//ファイルをデータフォルダに移動
				Files.move(TEMPFILE.toPath(), Paths.get(CONFIG_DATA.get("DIR").asString("DIR") + ID));
			} else {
				//同じハッシュのファイルが既に有る
				//同じハッシュのファイルを参照するように登録
				SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`, `FILE`, `HASH`) VALUES (?, ?, ?, ?, ?)", new Object[] {
					SnowFlake.GEN(),
					BUCKET,
					NAME,
					SQL_RESULT.get(0).asString("ID"),
					HASH_TEXT
				});

				//削除
				TEMPFILE.delete();
			}
		}
	}

	public static void DeleteFile(String BUCKET, String NAME) throws SQLException {
		String ID = GetID(BUCKET, NAME);
		String FID = GetFILEID(BUCKET, NAME);

		//ファイルを登録から削除
		SQL.UP_RUN("DELETE FROM `DATA` WHERE `DATA`.`ID` = ?;", new Object[] {ID});

		//同じデータを参照しているファイルが他に有るか
		if (SQL.RUN("SELECT * FROM `DATA` WHERE `FILE` = ?", new Object[] {FID}).asArrayList().size() == 0) {
			//無いので削除処理
			File FILE = new File(CONFIG_DATA.get("DIR").asString("PATH") + FID);
			//ファイルが存在するか
			if (FILE.exists()) {
				//削除
				FILE.delete();
			}
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

	private static String GetFILEID(String BUCKET, String NAME) {
		ArrayNode RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `DATA`.`BUCKET` = BINARY ? AND `DATA`.`NAME` = BINARY ?;", new Object[] {
			BUCKET,
			NAME
		});

		if (RESULT.asArrayList().size() == 1) {
			return RESULT.get(0).asString("FILE");
		} else {
			return null;
		}
	}
}
