package me.bigdata.kudu.ext.helper;

public class CustomerException extends RuntimeException {

    /**
     * 错误码
     */
    private String errorCode;
    /**
     * 错误上下文
     */
    private String errorContext;

    public CustomerException(String errorCode, String errorMsg) {
        super(errorMsg);
        this.errorCode = errorCode;
    }

    public CustomerException(ExceptionEnum orderExceptionEnum) {
        super(orderExceptionEnum.getErrorMsg());
        this.errorCode = orderExceptionEnum.getErrorCode();
    }

    public CustomerException(String errorCode, String errorMsg, Throwable throwable) {
        super(errorMsg, throwable);
        this.errorCode = errorCode;
    }

    public CustomerException(ExceptionEnum exceptionEnum, String errorMsg) {
        super(exceptionEnum.getErrorMsg() + " " + errorMsg);
        this.errorCode = exceptionEnum.getErrorCode();
        this.errorContext = exceptionEnum.getErrorMsg();
    }

    public CustomerException(ExceptionEnum exceptionEnum, Throwable throwable) {
        super(exceptionEnum.getErrorMsg(), throwable);
        this.errorCode = exceptionEnum.getErrorCode();
    }

    /**
     * Getter method for property <tt>errorCode</tt>.
     *
     * @return property value of errorCode
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Setter method for property <tt>errorCode</tt>.
     *
     * @param errorCode value to be assigned to property errorCode
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }


    /**
     * Getter method for property <tt>errorContext</tt>.
     *
     * @return property value of errorContext
     */
    public String getErrorContext() {
        return errorContext;
    }

    /**
     * Setter method for property <tt>errorContext</tt>.
     *
     * @param errorContext value to be assigned to property errorContext
     */
    public void setErrorContext(String errorContext) {
        this.errorContext = errorContext;
    }

    public static void main(String[] args) {
        try {
            int i = 1 / 0;
        } catch (Exception e) {
            throw new CustomerException(ExceptionEnum.SYSTEM_ERROR, e);
        }
    }
}