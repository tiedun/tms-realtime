package com.atguigu.tms.realtime.app.func;

import com.atguigu.tms.realtime.bean.DwsTransDispatchDayBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.lang.reflect.Field;

public class MyTriggerFunction<T> extends Trigger<T, TimeWindow> {
    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> isFirstState = ctx.getPartitionedState(
                new ValueStateDescriptor<Boolean>("is-first-value", Boolean.class));
        Boolean isFirst = isFirstState.value();
        if (isFirst == null) {
            isFirstState.update(true);
            Field tsField = element.getClass().getDeclaredField("ts");
            tsField.setAccessible(true);
            Long ts = (Long)tsField.get(element);
            long nextTime = ts + 10 * 1000L - ts % 10 * 1000L;
            ctx.registerEventTimeTimer(nextTime);
        } else if (isFirst) {
            isFirstState.update(false);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        long edt = window.getEnd();
        if (time < edt) {
            if (time + 10 * 1000L < edt) {
                ctx.registerEventTimeTimer(time + 10 * 1000L);
            }
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    }
}