package su.rumishistem.rumidataserver.MODULE;

import static su.rumishistem.rumi_java_lib.LOG_PRINT.Main.LOG;
import static su.rumishistem.rumidataserver.Main.CONFIG_DATA;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import su.rumishistem.rumi_java_lib.ArrayNode;
import su.rumishistem.rumi_java_lib.SQL;
import su.rumishistem.rumi_java_lib.LOG_PRINT.LOG_TYPE;

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
		LOG(LOG_TYPE.OK, "Read:" + ID);
		return Files.readAllBytes(Path.of(FILE_PATH));
	}

	public void Remove() throws SQLException, IOException {
		SQL.UP_RUN("DELETE FROM `DATA` WHERE `ID` = ?;", new Object[] {ID});
		Files.delete(Path.of(FILE_PATH));

		LOG(LOG_TYPE.OK, "Remove:" + ID);
	}

	public void Write(String BUCKET, String NAME, byte[] DATA, boolean APPEND) throws IOException, SQLException {
		if (!Files.exists(Path.of(FILE_PATH))) {
			//ファイルがないので作成
			SQL.UP_RUN("INSERT INTO `DATA` (`ID`, `BUCKET`, `NAME`, `PUBLIC`) VALUES (?, ?, ?, ?)", new Object[] {
				ID,
				BUCKET,
				NAME,
				false
			});

			Files.createFile(Path.of(FILE_PATH));
		}

		//書き込み
		FileOutputStream FOS = new FileOutputStream(new File(FILE_PATH), APPEND);
		FOS.write(DATA);
		FOS.close();

		LOG(LOG_TYPE.OK, "Write:" + ID);
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
