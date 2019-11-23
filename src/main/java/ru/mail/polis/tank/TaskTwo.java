package ru.mail.polis.tank;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TaskTwo {
    public static void main(String[] args) throws IOException {
        final int count = 1000000;
        final int valueLength = 256;
        final Random random = new Random();
        final float repeatProb = 0.1f;
        try (final FileOutputStream fileOutputStream = new FileOutputStream("tank/task2.txt")) {
            final List<String> keys = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final float repeat = random.nextFloat();
                String key;
                if (Float.compare(repeat, repeatProb) < 0 && keys.size() != 0) {
                    key = keys.get(random.nextInt(keys.size()));
                } else {
                    key = GeneratorUtil.randomKey();
                    keys.add(key);
                }
                keys.add(key);
                final byte[] value = GeneratorUtil.randomValue(valueLength);
                GeneratorUtil.put(fileOutputStream, key, value);
            }
        }
    }
}
