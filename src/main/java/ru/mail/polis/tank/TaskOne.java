package ru.mail.polis.tank;

import java.io.FileOutputStream;
import java.io.IOException;

public class TaskOne {
    static void main(final String[] args) throws IOException {
        final int count = 1000000;
        final int valueLength = 256;
        try (FileOutputStream fileOutputStream = new FileOutputStream("tank/task1.txt")) {
            for (int i = 0; i < count; i++) {
                final String key = GeneratorUtil.randomKey();
                final byte[] value = GeneratorUtil.randomValue(valueLength);
                GeneratorUtil.put(fileOutputStream, key, value);
            }
        }
    }
}
