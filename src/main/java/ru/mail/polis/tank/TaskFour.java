package ru.mail.polis.tank;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TaskFour {
    public static void main(String[] args) throws IOException {
        final int count = 1000000;
        final int valueLength = 256;
        final int skipKeys = (int) (count * 0.9f);
        final Random random = new Random();
        try (final FileOutputStream fileOutputStream = new FileOutputStream("tank/putTask4.txt");
             final FileOutputStream fileOutputStream1 = new FileOutputStream("tank/task4.txt")) {
            final List<String> keys = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final String key = GeneratorUtil.randomKey();
                if (i >= skipKeys) {
                    keys.add(key);
                }
                final byte[] value = GeneratorUtil.randomValue(valueLength);
                GeneratorUtil.put(fileOutputStream, key, value);
            }
            for (int i = 0; i < count; i++) {
                final String key = keys.get(random.nextInt(keys.size()));
                GeneratorUtil.get(fileOutputStream1, key);
            }
        }
    }
}
