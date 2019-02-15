package me.bigdata.kudu.ext.helper;

public enum ExceptionEnum {

    /**
     * 未知异常
     */
    UNKNOWN_EXCEPTION("OE001", "未知异常", "warn"),
    /**
     * 系统错误
     */
    SYSTEM_ERROR("OE002", "系统错误", "error"),
    /**
     * 操作错误
     */
    OPERATION_ERROR("OE003", "操作错误", "error"),
    ;

    private String errorCode;
    private String errorMsg;
    private String errorType;

    ExceptionEnum(String errorCode, String errorMsg, String errorType) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
        this.errorType = errorType;
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
     * Getter method for property <tt>errorMsg</tt>.
     *
     * @return property value of errorMsg
     */
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Setter method for property <tt>errorMsg</tt>.
     *
     * @param errorMsg value to be assigned to property errorMsg
     */
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     * Getter method for property <tt>errorType</tt>.
     *
     * @return property value of errorType
     */
    public String getErrorType() {
        return errorType;
    }

    /**
     * Setter method for property <tt>errorType</tt>.
     *
     * @param errorType value to be assigned to property errorType
     */
    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }
}
