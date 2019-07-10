import java.util.*;

/**
 * Created by root on 2018/4/2.
 */
public class Test {



    public static void main(String[] args){


        Map<String,Object> map = new HashMap();
        map.put("a",new Object());
        map.put("b",new Object());
        map.put("c",new Object());
        map.put("d",new Object());
        map.put("e",new Object());

        for (Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();){
            Map.Entry<String, Object> item = it.next();
            if (item.getKey().equals("c")){
                it.remove();
            }
        }
        System.out.println(map);

//        for (Map.Entry<String,Object> entry : map.entrySet()){
//            if (entry.getKey().equals("c")){
//                //map.remove(entry.getKey());
//                map.put("c",new Date());
//                //map.put("c",new Object());
//            }
//            //System.out.println(entry.getValue());
//        }
//        System.out.println(map);

//        Iterator<String> iterator = map.keySet().iterator();
//        while (iterator.hasNext()){
//            String key = (String) iterator.next();
//            if (key.equals("c")){
//                //iterator.remove();
//                //map.remove(key);
//                map.put("f",new Date());
//            }
//            System.out.println(map.get(key));
//        }
//        System.out.println(map);





    }




}
