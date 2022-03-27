package com.dahua.DIM

import org.apache.parquet.column.page.DataPage
import org.apache.spark.sql.catalyst.optimizer.LikeSimplification

import javax.validation.OverridesAttribute

object DIMZhibiao {
  def qqsRtp(requestMode: Int, productNode: Int) {
    if (requestMode == 1 && productNode >= 1) {
      List[Double](1, 0, 0)
    } else if (requestMode == 1 && productNode >= 2) {
      List[Double](1, 1, 0)
    } else if (requestMode == 1 && productNode == 3) {
      List[Double](1, 1, 1)
    }
  }

  //参与竞价数
  def jingjiaRtp(ecTive: Int, Bill: Int, bid: Int, isWin: Int, orderId: Int) {
    if (ecTive == 1 && Bill == 1 && bid == 1 && orderId != 0) {
      List[Double](1, 0)
    } else if (ecTive == 1 && Bill == 1 && isWin == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  //广告展示数
  def ggzjRtp(rMode:Int,ecTive :Int){
    if(rMode==2 && ecTive==1){
      List[Double](1,0)
    }else if(rMode==3&& ecTive==1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }
}

//媒介展示数

