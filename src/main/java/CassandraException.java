/**
 * Created by dongyan on 20/06/16.
 */
public class CassandraException extends Exception{
    public CassandraException(String message) {
        super(message);
    }

    public CassandraException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Smart constructor - if the last argument is an Throwable, if will be set as the cause for this exception.
     *
     * @param format
     * @param args
     */
    public CassandraException(String format, Object... args) {
        super(String.format(format, args));

        if (args.length > 0 && args[args.length - 1] instanceof Throwable) {
            super.initCause((Throwable) args[args.length - 1]);
        }
    }
}
