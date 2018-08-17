package cn.pojo;

/**
 * @author wangchen
 * @date 2018/8/16 10:30
 */
public class Customer {
    private Integer customerID;

    private String customerName;

    public Customer() {
    }

    public Customer(Integer customerID, String customerName) {
      this.customerID = customerID;
      this.customerName = customerName;
    }

    public Integer getCustomerID() {
      return customerID;
    }

    public void setCustomerID(Integer customerID) {
      this.customerID = customerID;
    }

    public String getCustomerName() {
      return customerName;
    }

    public void setCustomerName(String customerName) {
      this.customerName = customerName;
    }
}
