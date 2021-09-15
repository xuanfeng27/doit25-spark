package cn.doitedu.cache.demos;

import java.util.LinkedHashMap;
import java.util.Map;

public class 自定义LRU策略缓存 {

    private LinkedHashMap<String,String> cacheMap;
    private static int MAX_SIZE = 15;

    public 自定义LRU策略缓存(int initCapacity,float loadFactor,boolean accessOrder){
        this.cacheMap = new LinkedHashMap<String,String>(initCapacity,loadFactor,accessOrder){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size()>MAX_SIZE;
            }
        };
    }


    public String get(String key){
        return  cacheMap.get(key);
    }

    public String put(String key,String value){
        return cacheMap.put(key,value);
    }

    @Override
    public String toString() {
        return cacheMap.toString();
    }

    public static void main(String[] args) {
        自定义LRU策略缓存 cache = new 自定义LRU策略缓存(100, 0.75f, true);

        cache.put("1","1");
        cache.put("2","2");
        cache.put("3","2");
        cache.put("4","2");
        cache.put("5","2");
        cache.put("6","2");
        cache.put("7","2");
        cache.put("8","2");
        cache.put("9","2");
        cache.put("10","2");
        cache.put("11","2");
        cache.put("12","2");
        cache.put("13","2");

        // get方法，内部会调用一个调整链表顺序的逻辑
        cache.get("2");

        cache.put("14","2");
        cache.put("15","2");
        cache.put("16","2");
        cache.put("17","2");




        System.out.println(cache);

    }

}
