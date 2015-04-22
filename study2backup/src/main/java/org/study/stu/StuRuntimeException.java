package org.study.stu;

public class StuRuntimeException extends RuntimeException{
	public StuRuntimeException(Throwable cause) { super(cause); }
	public StuRuntimeException(String message) { super(message); }
	public StuRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}
}
