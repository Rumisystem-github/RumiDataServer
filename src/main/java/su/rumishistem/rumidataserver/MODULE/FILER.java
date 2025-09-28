package su.rumishistem.rumidataserver.MODULE;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;
import static su.rumishistem.rumidataserver.Main.CONFIG_DATA;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import io.netty.channel.ChannelHandlerContext;
import su.rumishistem.rumi_java_lib.*;
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

	public void Read(ChannelHandlerContext ctx) throws IOException {
		FileLoader.read(FILE_PATH, ctx, ID, FileID);
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
			SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`, `PUBLIC`, `FILE`) VALUES (?, ?, ?, ?, NULL)", new Object[] {
				ID,
				BUCKET,
				NAME,
				PUBLIC
			});
		}
	}

	//ファイルで上書き
	public void write_from_file(File file) throws IOException {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA3-512");
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(0);

			String old_fid = FileID;
			String fid = null;

			try {
				//ハッシュを生成
				byte[] hash_buffer = new byte[8024];
				int hash_length;
				while ((hash_length = raf.read(hash_buffer)) != -1) {
					md.update(hash_buffer, 0, hash_length);
				}

				//戻る
				raf.seek(0);

				//ハッシュを文字列化
				StringBuilder hash_sb = new StringBuilder();
				for (byte b:md.digest()) {
					hash_sb.append(String.format("%02x", b & 0xFF));
				}
				String hash = hash_sb.toString();
				fid = GetHashToFID(hash);

				//同じハッシュのファイルがあるか？
				if (fid != null) {
					//リンキング
					SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ? WHERE `DATA`.`ID` = ?; ", new Object[] {
						fid, ID
					});

					LOG(LOG_TYPE.OK, "Link:" + ID);
				} else {
					//ファイル作成
					fid = String.valueOf(SnowFlake.GEN());
					File original_file = new File(CONFIG_DATA.get("DIR").getData("PATH").asString() + fid);
					original_file.createNewFile();

					//書き込み
					FileOutputStream fos = new FileOutputStream(original_file, false);
					byte[] read_buffer = new byte[8024];
					int read_length;
					while ((read_length = raf.read(read_buffer)) != -1) {
						fos.write(read_buffer, 0, read_length);
						fos.flush();
					}
					fos.close();

					//SQLに保存
					SQL.UP_RUN("INSERT INTO `FILE` (`ID`, `HASH`) VALUES (?, ?)", new Object[] {
						fid, hash
					});

					//リンキング
					SQL.UP_RUN("UPDATE `DATA` SET `FILE` = ? WHERE `DATA`.`ID` = ?;", new Object[] {
						fid, ID
					});

					LOG(LOG_TYPE.OK, "Create Write:" + ID);
				}
			} finally {
				raf.close();
				FIDCheck(old_fid);
			}
		} catch (NoSuchAlgorithmException ex) {
			//死ね
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException("書き込みエラー");
		}
	}

	public boolean isPublic() {
		try {
			ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `ID` = ?;", new Object[] {ID});
			if (SQL_RESULT.asArrayList().size() == 1) {
				return SQL_RESULT.get(0).getData("PUBLIC").asBool();
			} else {
				return false;
			}
		} catch (Exception EX) {
			EX.printStackTrace();
			return false;
		}
	}

	private String GetFileID(String ID) {
		try {
			ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `DATA` WHERE `ID` = ?", new Object[] {ID});
			if (SQL_RESULT.asArrayList().size() == 1) {
				return SQL_RESULT.get(0).getData("FILE").asString();
			} else {
				return null;
			}
		} catch (Exception EX) {
			EX.printStackTrace();
			return null;
		}
	}

	private String GetHashToFID(String HASH) {
		try {
			ArrayNode SQL_RESULT = SQL.RUN("SELECT * FROM `FILE` WHERE `HASH` = ?;", new Object[] {HASH});
			if (SQL_RESULT.asArrayList().size() != 0) {
				return SQL_RESULT.get(0).getData("ID").asString();
			} else {
				return null;
			}
		} catch (Exception EX) {
			EX.printStackTrace();
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

					SQL.UP_RUN("DELETE FROM `FILE` WHERE `ID` = ?;", new Object[] {FID});

					LOG(LOG_TYPE.OK, "Remove Origin File" + FID);
				}
			}
		} catch (Exception EX) {
			EX.printStackTrace();
		}
	}
}
