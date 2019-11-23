package ru.mail.polis.tank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;

final class TaskOne {

    private static final Logger log = LoggerFactory.getLogger(TaskOne.class);

    private TaskOne() {
    }

    static void main(final String[] args) throws IOException {
        if (args.length != 0) {
            log.info("No need to add params");
        }
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
