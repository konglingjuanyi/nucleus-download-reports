package org.gooru.nucleus.reports.generator.component;

import java.io.*;
import java.net.URI;
import java.util.Deque;
import java.util.LinkedList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileGenerator {

    private static void copy(File file, OutputStream out) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            byte[] buffer = new byte[1024];
            while (true) {
                int readCount = in.read(buffer);
                if (readCount < 0) {
                    break;
                }
                out.write(buffer, 0, readCount);
            }
        }
    }

    private static void zip(File baseDirectory, File zipfile) throws IOException {
        URI uriOfBaseDirectory = baseDirectory.toURI();
        Deque<File> queueOfDirectoriesToBeZipped = new LinkedList<>();
        queueOfDirectoriesToBeZipped.push(baseDirectory);
        OutputStream out = new FileOutputStream(zipfile);
        Closeable res = out;
        try {
            ZipOutputStream zout = new ZipOutputStream(out);
            res = zout;
            while (!queueOfDirectoriesToBeZipped.isEmpty()) {
                baseDirectory = queueOfDirectoriesToBeZipped.pop();
                if (baseDirectory != null) {
                    for (File currentChildFile : baseDirectory.listFiles()) {
                        String name = uriOfBaseDirectory.relativize(currentChildFile.toURI()).getPath();
                        if (currentChildFile.isDirectory()) {
                            queueOfDirectoriesToBeZipped.push(currentChildFile);
                            name = name.endsWith("/") ? name : name + '/';
                            zout.putNextEntry(new ZipEntry(name));
                        } else {
                            zout.putNextEntry(new ZipEntry(name));
                            copy(currentChildFile, zout);
                            zout.closeEntry();
                        }
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
