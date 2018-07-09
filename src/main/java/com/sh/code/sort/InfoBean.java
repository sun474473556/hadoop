package com.sh.code.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化
 */
public class InfoBean implements WritableComparable<InfoBean> {

	//账号
	private String account;

	//收入
	private double income;

	//支出
	private double outcome;

	//结余
	private double surplus;

	public void set(String account, double income, double outcome) {
		this.account = account;
		this.income = income;
		this.outcome = outcome;
		this.surplus = surplus;
	}

	/**
	 * Compares this object with the specified object for order.  Returns a
	 * negative integer, zero, or a positive integer as this object is less
	 * than, equal to, or greater than the specified object.
	 *
	 * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
	 * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
	 * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
	 * <tt>y.compareTo(x)</tt> throws an exception.)
	 *
	 * <p>The implementor must also ensure that the relation is transitive:
	 * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
	 * <tt>x.compareTo(z)&gt;0</tt>.
	 *
	 * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
	 * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
	 * all <tt>z</tt>.
	 *
	 * <p>It is strongly recommended, but <i>not</i> strictly required that
	 * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
	 * class that implements the <tt>Comparable</tt> interface and violates
	 * this condition should clearly indicate this fact.  The recommended
	 * language is "Note: this class has a natural ordering that is
	 * inconsistent with equals."
	 *
	 * <p>In the foregoing description, the notation
	 * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
	 * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
	 * <tt>0</tt>, or <tt>1</tt> according to whether the value of
	 * <i>expression</i> is negative, zero or positive.
	 *
	 * @param o the object to be compared.
	 * @return a negative integer, zero, or a positive integer as this object
	 * is less than, equal to, or greater than the specified object.
	 * @throws NullPointerException if the specified object is null
	 * @throws ClassCastException   if the specified object's type prevents it
	 *                              from being compared to this object.
	 */
	public int compareTo(InfoBean o) {
		if (this.income == o.getIncome()) {
			return this.getOutcome()>o.getOutcome() ? 1:-1;
		} else {
			return this.getIncome()>o.getIncome() ? 1:-1;
		}

	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(account);
		dataOutput.writeDouble(income);
		dataOutput.writeDouble(outcome);
		dataOutput.writeDouble(surplus);
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.account = dataInput.readUTF();
		this.income = dataInput.readDouble();
		this.outcome = dataInput.readDouble();
		this.surplus = dataInput.readDouble();
	}

	@Override
	public String toString() {
		return this.income+"\t"+this.outcome+"\t"+this.surplus;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double income) {
		this.income = income;
	}

	public double getOutcome() {
		return outcome;
	}

	public void setOutcome(double outcome) {
		this.outcome = outcome;
	}

	public double getSurplus() {
		return surplus;
	}

	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}
}
