
* `Watermark`用于标记流计算中，当前的计算进度
* `Watermark`由`WatermarkAssignerRunnable`算子生成
* 由`StreamStatus`事件触发，并转换`StreamStatus`为`Watermark`继续在流中传递，`StreamStatus`中的属性会保留到`Watermark`中，主要用于事件对齐
* `Watermark`和`Record`一样具有窗口属性，事件流经`WindowAssignerRunnable`会为其计算出窗口，该窗口用于`ReduceRunnable`窗口drop的条件依据

