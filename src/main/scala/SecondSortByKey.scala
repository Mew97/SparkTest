class SecondSortByKey(val first: Int, val second: Int) extends Ordered[SecondSortByKey] with Serializable {
  def compare(other: SecondSortByKey): Int = {
    //this关键字可加，也可不加，如果遇到多个变量时，必须添加
    if (this.first - other.first != 0)
      this.first - other.first
    else
      this.second - other.second
  }
}
