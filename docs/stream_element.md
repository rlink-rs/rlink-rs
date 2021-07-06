
## Record
* 包装业务数据，真正用于计算的数据
* 数据包含timestamp熟悉，具有窗口属性，事件流经`WindowAssignerRunnable`会为其计算出窗口

## StreamStatus
* `StreamStatus`作为周期性的事件注入到计算流中
* 触发`Watermark`，在触发后`StreamStatus`将停止在流中传递
* 标记流是否结束
* 在事件对齐上，`StreamStatus`与`Watermark`具有同等效应，都可标记事件是否到来（因为`Watermark`是由`StreamStatus`触发转换而来）

## Watermark
* `Watermark`推进时间进度，触发窗口计算
* `Watermark`由`WatermarkAssignerRunnable`算子生成
* 由`StreamStatus`事件触发，并转换`StreamStatus`为`Watermark`继续在流中传递，`StreamStatus`中的属性会保留到`Watermark`中，主要用于事件对齐
* `Watermark`和`Record`一样具有窗口属性，事件流经`WindowAssignerRunnable`会为其计算出窗口，该窗口用于`ReduceRunnable`窗口drop的条件依据

## Barrier
* `Barrier`作为周期性的事件注入到计算流中
* `Barrier`经过算子时，会触发算子的`Checkpoint`
