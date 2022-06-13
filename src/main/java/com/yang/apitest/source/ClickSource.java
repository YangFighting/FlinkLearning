package com.yang.apitest.source;

import com.yang.apitest.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author zhangyang03
 * @Description
 * @create 2022-05-25 20:35
 */
public class ClickSource implements SourceFunction<Event> {

    // 控制标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while(running){
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
