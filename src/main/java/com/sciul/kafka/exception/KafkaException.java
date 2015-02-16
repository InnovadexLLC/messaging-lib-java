package com.sciul.kafka.exception;

/**
 * The Class kafkaException.
 * 
 * @author GauravChawla
 */
public class KafkaException extends Exception {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -7345365257887387856L;

  /** The code. */
  private Integer code;

  /**
   * Instantiates a new kafka exception.
   * 
   * @param message
   *          the message
   * @param code
   *          the code
   * @param e
   *          the e
   */
  public KafkaException(String message, Integer code, Exception e) {
    super(message, e);
    this.code = code;
  }

  /**
   * Instantiates a new kafka exception.
   * 
   * @param message
   *          the message
   */
  public KafkaException(String message) {
    super(message);
  }

  /**
   * Instantiates a new kafka exception.
   * 
   * @param message
   *          the message
   * @param e
   *          the e
   */
  public KafkaException(String message, Exception e) {
    super(message, e);
  }

  /**
   * Instantiates a new kafka exception.
   * 
   * @param message
   *          the message
   * @param code
   *          the code
   */
  public KafkaException(String message, Integer code) {
    super(message);
    this.code = code;
  }

  /**
   * Gets the code.
   * 
   * @return the code
   */
  public Integer getCode() {
    return code;
  }

  /**
   * Sets the code.
   * 
   * @param code
   *          the new code
   */
  public void setCode(Integer code) {
    this.code = code;
  }

}
