package io.kestra.plugin.dbt;

import io.kestra.core.runners.RunContext;

import java.lang.reflect.Field;
import java.util.Optional;

public final class RunContextUtils {
    private RunContextUtils() {
    }

    public static void ensureSecretKey(RunContext runContext) {
        if (runContext == null) {
            return;
        }

        try {
            Class<?> type = runContext.getClass();
            Field field = null;
            while (type != null && field == null) {
                try {
                    field = type.getDeclaredField("secretKey");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }

            if (field == null) {
                return;
            }

            initializeRunContext(runContext);
            field.setAccessible(true);
            if (field.get(runContext) == null) {
                field.set(runContext, Optional.empty());
            }
        } catch (IllegalAccessException e) {
            // Ignore to avoid breaking task execution on reflection issues.
        }
    }

    private static void initializeRunContext(RunContext runContext) {
        try {
            Field appField = null;
            Class<?> type = runContext.getClass();
            while (type != null && appField == null) {
                try {
                    appField = type.getDeclaredField("applicationContext");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }
            if (appField == null) {
                return;
            }
            appField.setAccessible(true);
            Object appContext = appField.get(runContext);
            if (appContext == null) {
                return;
            }
            var initMethod = runContext.getClass().getDeclaredMethod("init", io.micronaut.context.ApplicationContext.class);
            initMethod.setAccessible(true);
            initMethod.invoke(runContext, appContext);
        } catch (ReflectiveOperationException e) {
            // Ignore to avoid breaking task execution on reflection issues.
        }
    }
}
