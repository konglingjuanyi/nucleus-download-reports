package org.gooru.nucleus.reports.infra.downlod.service;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.gooru.nucleus.reports.infra.component.UtilityManager;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;

public class ZipFileGenerator {
	private static UtilityManager um = UtilityManager.getInstance();

	public void zipDir(String zipFileName, String directoryName) throws Exception {
		File dirObj = new File(directoryName);
		File f = new File(zipFileName);
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
		System.out.println("Creating : " + zipFileName);
		addDir(dirObj, out);
		out.close();
	}

	private void addDir(File dirObj, ZipOutputStream out) throws IOException {
		File[] files = dirObj.listFiles();
		byte[] tmpBuf = new byte[1024];

		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory()) {
				addDir(files[i], out);
				continue;
			}
			FileInputStream in = new FileInputStream(files[i].getAbsolutePath());
			System.out.println(" Adding: " + files[i].getAbsolutePath());
			out.putNextEntry(new ZipEntry(files[i].getAbsolutePath()));
			int len;
			while ((len = in.read(tmpBuf)) > 0) {
				out.write(tmpBuf, 0, len);
			}
			out.closeEntry();
			in.close();
		}
	}
	
	private String setFilePath(String file){
		
		String fileName = um.getFileSaveRealPath();
		if(file != null && (!file.isEmpty())){
			fileName += file;
		}else{
			fileName +=DEFAULT_FILE_NAME;
		}
		return fileName+ConfigConstants.CSV_EXT;
	}
}
