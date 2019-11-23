package ru.mail.polis.tank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static ru.mail.polis.tank.GeneratorUtil.randomKey;

final class TaskFive {

    private static final Logger log = LoggerFactory.getLogger(TaskThree.class);

    private TaskFive() {
    }

    static void main(final String[] args) throws IOException {
        if (args.length != 0) {
            log.info("No need to add params");
        }
        final int count = 1000000;
        final int valueLength = 256;
        final Random random = new Random();
        try (FileOutputStream fileOutputStream = new FileOutputStream("tank/task5.txt")) {
            final List<String> keys = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (random.nextBoolean() && !keys.isEmpty()) {
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
