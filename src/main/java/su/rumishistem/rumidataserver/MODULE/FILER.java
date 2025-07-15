package su.rumishistem.rumidataserver.MODULE;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;
import static su.rumishistem.rumidataserver.Main.CONFIG_DATA;
import static su.rumishistem.rumidataserver.Main.FC;

import java.io.*;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.UUID;

import su.rumishistem.rumi_java_lib.*;
import su.rumishistem.rumi_java_lib.HASH.HASH_TYPE;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;

public class FILER {
	private String ID = null;
	private String FileID = null;
	private String FILE_PATH = null;

	public FILER(String ID) {
		this.ID = ID;
		this.FileID = GetFileID(ID);
		this.FILE_PATH = CONFIG_DATA.get("DIR").getData("PATH").asString() + FileID;
	}

	public boolean exists() {
		return Files.exists(Path.of(FILE_PATH));
	}

	public String getFilePath() {
		return FILE_PATH;
	}

	public byte[] Read() throws IOException {
		byte[] Data = FC.get(FILE_PATH);

		if (Data == null) {
			Data = Files.readAllBytes(Path.of(FILE_PATH));
			FC.put(FILE_PATH, Data);

			LOG(LOG_TYPE.OK, "Load ID:" + ID + " FileID:" + FileID);
		} else {
			LOG(LOG_TYPE.OK, "Read ID:" + ID + " FileID:" + FileID);
		}

		return Data;
	}

	public void Remove() throws SQLException, IOException {
		String FID = GetFileID(ID);
		SQL.UP_RUN("DELETE FROM `DATA` WHERE `ID` = ?;", new Object[] {ID});
		FIDCheck(FID);

		LOG(LOG_TYPE.OK, "Remove:" + ID);
	}

	public void Create(String BUCKET, String NAME, boolean PUBLIC) throws SQLException, IOException {
		if (FileID == null) {
			//ファイルがないので作成
			SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`, `PUBLIC`, `FILE`, `HASH`) VALUES (?, ?, ?, ?, NULL, '')", new Object[] {
				ID,
				BUCKET,
				NAME,
				PUBLIC
			});
		}
	}

	public void Write(byte[] DATA, boolean APPEND) {
		if (APPEND) {
			AppendWrite(DATA);
		} else {
			OverWrite(DATA);
		}
	}

	public boolean isPublic() {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `ID` = ?;", new Object[] {ID});
		if (SQL_RESULT.asArrayList().size() == 1) {
			return SQL_RESULT.get(0).getData("PUBLIC").asBool();
		} else {
			return false;
		}
	}

	public void AppendWrite(byte[] DATA) {
		try {
			String TempPath = "/tmp/rds-" + UUID.randomUUID().toString();
			String SourceFile = CONFIG_DATA.get("DIR").getData("PATH").asString() + GetFileID(ID);
			String OldFID = FileID;
			if (Files.exists(Path.of(SourceFile))) {
				//一時ファイルにコピー
				Files.copy(Path.of(SourceFile), Path.of(TempPath));

				//追記
				FileOutputStream FOS = new FileOutputStream(new File(TempPath), true);
				FOS.write(DATA);
				FOS.close();

				//ハッシュ生成したり
				String HR = HASH.Gen(HASH_TYPE.SHA3_512, Files.readAllBytes(Path.of(TempPath)));
				String FID = GetHashToFID(HR);

				if (FID != null) {
					//ハッシュが同じファイルがあるのでそれを参照するように設定
					SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ?, `HASH` = ? WHERE `DATA`.`ID` = ?; ", new Object[] {
						FID, HR, ID
					});

					LOG(LOG_TYPE.OK, "Link:" + ID);
				} else {
					//FIDを新規作成
					FID = String.valueOf(SnowFlake.GEN());

					//同じハッシュのファイルがないのでファイルを一時ファイルをコピー
					Files.copy(Path.of(TempPath), Path.of(CONFIG_DATA.get("DIR").getData("PATH").asString() + FID));

					//SQLにFIDカキコ
					SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ?, `HASH` = ? WHERE `DATA`.`ID` = ?; ", new Object[] {
						FID, HR, ID
					});

					LOG(LOG_TYPE.OK, "Create Write:" + ID);
				}

				//FIDチェック
				FIDCheck(OldFID);
			} else {
				throw new Error("ファイルがありません。");
			}
		} catch (Exception EX) {
			EX.printStackTrace();
			throw new Error("書き込みエラー");
		}
	}

	private void OverWrite(byte[] DATA) {
		try {
			//ハッシュ生成
			String HR = HASH.Gen(HASH_TYPE.SHA3_512, DATA);
			String OldFID = FileID;
			String FID = GetHashToFID(HR);

			if (FID != null) {
				//ハッシュが同じファイルがあるのでそれを参照するように設定
				SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ?, `HASH` = ? WHERE `DATA`.`ID` = ?; ", new Object[] {
					FID, HR, ID
				});

				LOG(LOG_TYPE.OK, "Link:" + ID);
			} else {
				//同じハッシュのファイルがないのでファイルを新規作成
				FID = String.valueOf(SnowFlake.GEN());
				FileOutputStream FOS = new FileOutputStream(new File(CONFIG_DATA.get("DIR").getData("PATH").asString() + FID), false);
				FOS.write(DATA);
				FOS.close();

				//SQLにFIDカキコ
				SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ?, `HASH` = ? WHERE `DATA`.`ID` = ?; ", new Object[] {
					FID, HR, ID
				});

				LOG(LOG_TYPE.OK, "Create Write:" + ID);
			}

			//FIDチェック
			FIDCheck(OldFID);
		} catch (Exception EX) {
			EX.printStackTrace();
			throw new Error("書き込みエラー");
		}
	}

	private String GetFileID(String ID) {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `ID` = ?", new Object[] {ID});
		if (SQL_RESULT.asArrayList().size() == 1) {
			return SQL_RESULT.get(0).getData("FILE").asString();
		} else {
			return null;
		}
	}

	private String GetHashToFID(String HASH) {
		ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `HASH` = ?;", new Object[] {HASH});
		if (SQL_RESULT.asArrayList().size() != 0) {
			return SQL_RESULT.get(0).getData("FILE").asString();
		} else {
			return null;
		}
	}

	private void FIDCheck(String FID) {
		try {
			ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `FILE` = ?;", new Object[] {FID});
			if (SQL_RESULT.asArrayList().size() == 0) {
				//同じ元ファイルを共有しているファイルがないので、ファイルを消す
				Path FilePath = Path.of(CONFIG_DATA.get("DIR").getData("PATH").asString() + FID);

				if (Files.exists(FilePath)) {
					Files.delete(FilePath);

					LOG(LOG_TYPE.OK, "Remove Origin File" + FID);
				}
			}
		} catch (Exception EX) {
			EX.printStackTrace();
		}
	}
}
