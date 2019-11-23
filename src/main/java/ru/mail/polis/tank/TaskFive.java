package ru.mail.polis.tank;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static ru.mail.polis.tank.GeneratorUtil.randomKey;

public class TaskFive {
    public static void main(String[] args) throws IOException {
        final int count = 1000000;
        final int valueLength = 256;
        final Random random = new Random();
        try (final FileOutputStream fileOutputStream = new FileOutputStream("tank/task5.txt")) {
            final List<String> keys = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (random.nextBoolean() && keys.size() != 0) {
                    final String key = keys.get(random.nextInt(keys.size()));
                    GeneratorUtil.get(fileOutputStream, key);
                } else {
                    final String key = randomKey();
                    keys.add(key);
                    final byte[] value = GeneratorUtil.randomValue(valueLength);
                    GeneratorUtil.put(fileOutputStream, key, value);
                }
            }
        }
    }
}
