import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.*;
import java.util.*;

/** Reflection keeps broker client JARs out of the repository. Source-run with Java 17. */
public class RelayEmsBridge {
    static String ns;
    static Object call(String type, Object target, String method, Class<?>[] types, Object... args) throws Exception {
        return Class.forName(type).getMethod(method, types).invoke(target, args);
    }
    static Object jms(String type, Object target, String method, Class<?>[] types, Object... args) throws Exception {
        return call(ns + "." + type, target, method, types, args);
    }
    static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "\\r").replace("\n", "\\n");
    }
    static void output(String json) {
        System.out.println("RELAY_RESULT " + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
    }
    public static void main(String[] args) {
        Object connection = null, session = null;
        int exitCode = 0;
        try {
            Properties encoded = new Properties();
            encoded.load(System.in);
            Map<String, String> values = new HashMap<>();
            for (String key : encoded.stringPropertyNames())
                values.put(key, new String(Base64.getDecoder().decode(encoded.getProperty(key)), StandardCharsets.UTF_8));
            ns = values.get("namespace");
            if (!ns.equals("javax.jms") && !ns.equals("jakarta.jms")) throw new IllegalArgumentException();
            Class<?> factoryType = Class.forName("com.tibco.tibjms.TibjmsConnectionFactory");
            Object factory = factoryType.getConstructor(String.class).newInstance(values.get("url"));
            if (values.get("url").startsWith("ssl://")) {
                factoryType.getMethod("setSSLTrustedCertificate", byte[].class, String.class).invoke(factory, values.get("trustedCertificate").getBytes(StandardCharsets.UTF_8), "PEM");
                factoryType.getMethod("setSSLEnableVerifyHost", Boolean.class).invoke(factory, Boolean.TRUE);
                factoryType.getMethod("setSSLEnableVerifyHostName", Boolean.class).invoke(factory, Boolean.TRUE);
            }
            connection = jms("ConnectionFactory", factory, "createConnection", new Class<?>[]{String.class, String.class}, values.get("username"), values.get("password"));
            if (values.get("operation").equals("test")) {
                output("{\"connected\":true,\"detail\":\"Authenticated JMS connection established; publishing permissions are checked on send\"}");
                return;
            }
            session = jms("Connection", connection, "createSession", new Class<?>[]{boolean.class, int.class}, true, 0);
            String destinationMethod = values.get("destinationType").equals("topic") ? "createTopic" : "createQueue";
            Object destination = jms("Session", session, destinationMethod, new Class<?>[]{String.class}, values.get("destination"));
            byte[] data = Base64.getDecoder().decode(values.get("data"));
            if (data.length > 10 * 1024 * 1024) throw new IllegalArgumentException("oversized");
            Object message;
            if (values.get("messageType").equals("text")) {
                String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(data)).toString();
                message = jms("Session", session, "createTextMessage", new Class<?>[]{String.class}, text);
            } else {
                message = jms("Session", session, "createBytesMessage", new Class<?>[]{});
                jms("BytesMessage", message, "writeBytes", new Class<?>[]{byte[].class}, data);
            }
            jms("Message", message, "setStringProperty", new Class<?>[]{String.class, String.class}, "RelayFilename", values.get("filename"));
            jms("Message", message, "setStringProperty", new Class<?>[]{String.class, String.class}, "RelayContentType", values.get("contentType"));
            Object producer = jms("Session", session, "createProducer", new Class<?>[]{Class.forName(ns + ".Destination")}, destination);
            jms("MessageProducer", producer, "setDeliveryMode", new Class<?>[]{int.class}, 2);
            jms("MessageProducer", producer, "send", new Class<?>[]{Class.forName(ns + ".Message")}, message);
            jms("Session", session, "commit", new Class<?>[]{});
            String messageId = (String) jms("Message", message, "getJMSMessageID", new Class<?>[]{});
            output("{\"committed\":true,\"delivery_mode\":\"persistent\",\"message_id\":\"" + escape(messageId) + "\"}");
        } catch (Throwable failure) {
            if (session != null) try { jms("Session", session, "rollback", new Class<?>[]{}); } catch (Exception ignored) {}
            // Provider exception text can contain connection details. Return a safe category only.
            System.err.println("EMS bridge failed: " + failure.getClass().getSimpleName());
            exitCode = 1;
        } finally {
            if (connection != null) try { jms("Connection", connection, "close", new Class<?>[]{}); } catch (Exception ignored) {}
        }
        if (exitCode != 0) System.exit(exitCode);
    }
}
