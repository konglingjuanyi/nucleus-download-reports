package org.gooru.nucleus.reports.infra.downlod.service;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileGenerator {
	public ZipOutputStream createZipFile(String zipFileName) {
		File f = new File(zipFileName);
		ZipOutputStream out = null;
		try {
			out = new ZipOutputStream(new FileOutputStream(f));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return out;
	}
	public void addFileInZip(String csvName, ZipOutputStream zip){
		try {
			ZipEntry e = new ZipEntry(csvName);
			zip.putNextEntry(e);
		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}
}
