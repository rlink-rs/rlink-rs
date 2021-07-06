
* `StreamStatus`作为周期性的事件注入到计算流中
* 触发`Watermark`，在触发后`StreamStatus`将停止在流中传递
* 标记流是否结束
* 在事件对齐上，`StreamStatus`与`Watermark`具有同等效应，都可标记事件是否到来（因为`Watermark`是由`StreamStatus`触发转换而来）

