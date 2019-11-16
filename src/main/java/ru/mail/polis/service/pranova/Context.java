package ru.mail.polis.service.pranova;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;

public class Context {
    private final HttpSession session;
    private final boolean isProxy;
    private final Request request;
    private final Replicas rf;

    public Context(@NotNull final HttpSession session,
                   final boolean isProxy,
                   @NotNull final Request request,
                   @NotNull final Replicas rf) {
        this.session = session;
        this.isProxy = isProxy;
        this.request = request;
        this.rf = rf;
    }

    public Replicas getRf() {
        return rf;
    }

    public HttpSession getSession() {
        return session;
    }

    public boolean isProxy() {
        return isProxy;
    }

    public Request getRequest() {
        return request;
    }
}
