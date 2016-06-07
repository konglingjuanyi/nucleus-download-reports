package org.gooru.nucleus.reports.generator.component;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Deque;
import java.util.LinkedList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileGenerator {
	
	private void copy(File file, OutputStream out) throws IOException {
		InputStream in = new FileInputStream(file);
		try {
			byte[] buffer = new byte[1024];
			while (true) {
				int readCount = in.read(buffer);
				if (readCount < 0) {
					break;
				}
				out.write(buffer, 0, readCount);
			}
		} finally {
			in.close();
		}
	}

	private void zip(File baseDirectory, File zipfile) throws IOException {
		URI uriOfBaseDirectory = baseDirectory.toURI();
		Deque<File> queueOfDirectoriesToBeZipped = new LinkedList<File>();
		queueOfDirectoriesToBeZipped.push(baseDirectory);
		OutputStream out = new FileOutputStream(zipfile);
		Closeable res = out;
		try {
			ZipOutputStream zout = new ZipOutputStream(out);
			res = zout;
			while (!queueOfDirectoriesToBeZipped.isEmpty()) {
				baseDirectory = queueOfDirectoriesToBeZipped.pop();
				for (File currentChildFile : baseDirectory.listFiles()) {
					String name = uriOfBaseDirectory.relativize(currentChildFile.toURI()).getPath();
					if (currentChildFile.isDirectory()) {
						queueOfDirectoriesToBeZipped.push(currentChildFile);
						name = name.endsWith("/") ? name : name + "/";
						zout.putNextEntry(new ZipEntry(name));
					} else {
						zout.putNextEntry(new ZipEntry(name));
						copy(currentChildFile, zout);
						zout.closeEntry();
					}
				}
			}
		} finally {
			res.close();
		}
	}

	public void zipDirectory(String zipFile, String sourceDirectory) throws IOException {
			File fout = new File(zipFile);
			File dir = new File(sourceDirectory);
			zip(dir, fout);
	}
}
