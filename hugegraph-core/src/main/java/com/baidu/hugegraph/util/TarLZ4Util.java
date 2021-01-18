/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.hugegraph.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Reference: https://mkyong.com/java/how-to-create-tar-gz-in-java/
 */
public final class TarLZ4Util {

    public static void compress(String rootDir, String sourceDir,
                                String outputFile, Checksum checksum)
                                throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
             BufferedOutputStream bos = new BufferedOutputStream(cos);
             LZ4BlockOutputStream lz4os = new LZ4BlockOutputStream(bos, 8192,
                                                                   compressor);
             TarArchiveOutputStream tos = new TarArchiveOutputStream(lz4os)) {
            Path source = Paths.get(rootDir, sourceDir);
            TarLZ4Util.tarDirectory(source, tos);
            tos.flush();
            fos.getFD().sync();
        }
    }

    private static void tarDirectory(Path source, TarArchiveOutputStream tos)
                                     throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                                                     BasicFileAttributes attrs)
                                                     throws IOException {
                String entryName = buildEntryName(source, dir);
                if (!entryName.isEmpty()) {
                    TarArchiveEntry entry = new TarArchiveEntry(dir.toFile(),
                                                                entryName);
                    tos.putArchiveEntry(entry);
                    tos.closeArchiveEntry();
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attributes)
                                             throws IOException {
                // Only copy files, no symbolic links
                if (attributes.isSymbolicLink()) {
                    return FileVisitResult.CONTINUE;
                }
                String targetFile = buildEntryName(source, file);
                TarArchiveEntry entry = new TarArchiveEntry(file.toFile(),
                                                            targetFile);
                tos.putArchiveEntry(entry);
                Files.copy(file, tos);
                tos.closeArchiveEntry();
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) {
                return FileVisitResult.TERMINATE;
            }
        });
    }

    private static String buildEntryName(Path topLevel, Path current) {
        return topLevel.getFileName().resolve(topLevel.relativize(current))
                       .toString();
    }

    public static void decompress(String sourceFile, String outputDir,
                                  Checksum checksum) throws IOException {
        Path source = Paths.get(sourceFile);
        Path target = Paths.get(outputDir);
        if (Files.notExists(source)) {
            throw new IOException("File doesn't exists");
        }
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        try (InputStream fis = Files.newInputStream(source);
             CheckedInputStream cis = new CheckedInputStream(fis, checksum);
             BufferedInputStream bis = new BufferedInputStream(cis);
             LZ4BlockInputStream lz4is = new LZ4BlockInputStream(bis,
                                                                 decompressor);
             TarArchiveInputStream tis = new TarArchiveInputStream(lz4is)) {
            ArchiveEntry entry;
            while ((entry = tis.getNextEntry()) != null) {
                // Create a new path, zip slip validate
                Path newPath = zipSlipProtect(entry, target);
                if (entry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    // check parent folder again
                    Path parent = newPath.getParent();
                    if (parent != null) {
                        if (Files.notExists(parent)) {
                            Files.createDirectories(parent);
                        }
                    }
                    // Copy TarArchiveInputStream to Path newPath
                    Files.copy(tis, newPath,
                               StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static Path zipSlipProtect(ArchiveEntry entry, Path targetDir)
                                       throws IOException {
        Path targetDirResolved = targetDir.resolve(entry.getName());
        /*
         * Make sure normalized file still has targetDir as its prefix,
         * else throws exception
         */
        Path normalizePath = targetDirResolved.normalize();
        if (!normalizePath.startsWith(targetDir)) {
            throw new IOException("Bad entry: " + entry.getName());
        }
        return normalizePath;
    }
}
