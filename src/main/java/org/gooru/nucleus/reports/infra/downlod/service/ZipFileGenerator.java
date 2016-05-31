package org.gooru.nucleus.reports.infra.downlod.service;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Deque;
import java.util.LinkedList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.gooru.nucleus.reports.infra.component.UtilityManager;

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
	//Ashish code.
	
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

    public void zipDirectoryNew(String zipFile, String sourceDirectory) {
        try {
            File fout = new File(zipFile);
            File dir = new File(sourceDirectory);
            zip(dir, fout);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
