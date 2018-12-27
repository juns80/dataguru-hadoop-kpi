package mr;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Created by juns on 2018/12/26
 */
public class KPI {
    private String remote_addr;    //客户端地址
    private String remote_user;    //客户端用户
    private String time_local;      //时间
    private String request;         //请求+协议
    private String status;          //状态码
    private String body_bytes_sent; //请求大小
    private String http_referer;    //跳转页
    private String http_user_agent; //浏览器信息

    private boolean isVaild = true;    //是否合法

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isVaild() {
        return isVaild;
    }

    public void setVaild(boolean vaild) {
        isVaild = vaild;
    }

    public Date getTime_local_Date() {
        try{
            SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
            return df.parse(this.time_local);
        } catch (Exception e)
        {   e.printStackTrace();  }
        return null;
    }

    public String getTime_local_hour(){
        try{
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
            return df.format(this.getTime_local_Date());
        } catch (Exception e)
        {   e.printStackTrace();  }
        return null;
    }
    
    public String getHttp_referer_domain(){
        if(http_referer.length()<8){
            return http_referer;
        }

        String str=this.http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
        return str.indexOf("/")>0?str.substring(0, str.indexOf("/")):str;
    }

    /**
     * 将日志记录转换成KPI对象
     * @param line
     * @return
     */
    public static KPI parse(String line){
        KPI kpi = new KPI();
        String[] arr = line.split(" ");
        if(arr.length>11){
            kpi.setRemote_addr(arr[0]);
            kpi.setRemote_user(arr[1]);
            kpi.setTime_local(arr[3].substring(1));
            kpi.setRequest(arr[6]);
            kpi.setStatus(arr[8]);
            kpi.setBody_bytes_sent(arr[9]);
            kpi.setHttp_referer(arr[10]);

            if(arr.length > 12){
                kpi.setHttp_user_agent(arr[11]+" "+arr[12]);
            }
            kpi.setHttp_user_agent(arr[11]);

            if(Integer.parseInt(kpi.getStatus()) >=400){
                kpi.setVaild(false);
            }
        }else{
            kpi.setVaild(false);
        }
        return kpi;
    }

    /**
     * 不符合集合里面的请求页面, 则过滤
     * @param line
     * @return
     */
    public static KPI filterPVs(String line){
        KPI kpi = parse(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())){
            kpi.setVaild(false);
        }
        return kpi;
    }
    /**
     * 不符合集合里面的请求页面, 则过滤
     * @param line
     * @return
     */
    public static KPI filterIPs(String line){
        KPI kpi = parse(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())){
            kpi.setVaild(false);
        }
        return kpi;
    }

    /**
     * 统计单位时间访问数量
     * @param line
     * @return
     */
    public static KPI filterTime(String line){
        return parse(line);
    }

    /**
     * 统计跳转数量
     * @param line
     * @return
     */
    public static KPI filterSource(String line){
        return parse(line);
    }

    /**
     * 统计浏览器类型数量
     * @param line
     * @return
     */
    public static KPI filterBrowser(String line){
        return parse(line);
    }




    @Override
    public String toString() {
        return "KPI{" +
                "remote_addr='" + remote_addr + '\'' +
                ", remote_user='" + remote_user + '\'' +
                ", time_local='" + time_local + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", body_bytes_sent='" + body_bytes_sent + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", isVaild=" + isVaild +
                '}';
    }

    /**
     * 测试
     * @param args
     */
    public static void main(String[] args){
        String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
        KPI kpi = KPI.parse(line);
        System.out.println(kpi);
        //System.out.println(df.format(kpi.getTime_local_Date()));
        System.out.println(kpi.getTime_local_hour());
        System.out.println(kpi.getHttp_referer_domain());
    }

}
