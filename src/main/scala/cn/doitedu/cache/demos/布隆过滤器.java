package cn.doitedu.cache.demos;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;

public class 布隆过滤器 {

    public static void main(String[] args) {


        // funnel:是要记录的数据的原始类型
        // expectedInsertions:预计要记录的数据的个数
        // fpp:false positive probability 假阳率 ： 本来不存在被误判为存在的概率
        BloomFilter<CharSequence> bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 1000000, 0.01);

        // 比如，我们往数据库里面插入了一个id=0005,0006,0010,0020,0025的商品，那么就要同时往布隆过滤器中进行记录
        bloomFilter.put("000000005");
        bloomFilter.put("000000006");
        bloomFilter.put("000000010");
        bloomFilter.put("000000020");
        bloomFilter.put("000000025");


        HashSet<Object> keys = new HashSet<>();
        keys.addAll(Arrays.asList("000000005","000000006","000000010","000000020","000000025"));

        // 以后，就可以通过布隆过滤器来判断某条数据是否存在
        for(int i=0;i<100000000;i++) {
            String key = StringUtils.leftPad(i+"", 9, "0");
            boolean b = bloomFilter.mightContain(key);
            if(b && !keys.contains(key)) {
                System.out.println(key);
            }

        }

    }
}
