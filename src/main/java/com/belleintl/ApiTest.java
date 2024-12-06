package com.belleintl;

import com.lesoon.bdp.api.lgt.Demo;
//import com.lesoon.bdp.api.ordertob.request.OmIntransitDaysKpiRequest;
//import com.lesoon.bdp.api.ordertob.response.OmIntransitDaysKpi;
import com.lesoon.bdp.core.client.DwApiClient;
import com.lesoon.bdp.core.util.Paginator;
import com.lesoon.bdp.core.util.ResponseBody;
import com.lesoon.bdp.data.service.client.dto.MdmOrgRelationshipDto;
import com.lesoon.petrel.common.vo.BizResponseVO;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import okhttp3.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.ServerCookieEncoder.encode;
import com.lesoon.bdp.data.service.client.api.MdmOrgRelationshipApi;
import com.lesoon.bdp.data.service.client.vo.MdmOrgRelationshipVo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @ClassName: ApiTest
 * @Description: API签名测试
 * @Author: zhipengl01
 * @Date: 2022/6/9
 */

public class ApiTest {

    public static void HmacSHA1() throws Exception {
        String httpMonth = "GET\n";
        String accept = "*/*\n";
        String contentMD5 = "\n";
        String contentType = "\n";
        String date = "\n";
        String headers = "x-ca-key:204069071\nx-ca-signature-method:HmacSHA256\n";
        String pathAndParameters = "/tobImOrderStatistic?end_create_time=2022-06-07 23:59:59&owner_no=01&start_create_time=2022-05-01 00:00:00";

        String httpReq = httpMonth+accept+contentMD5+contentType+date+headers+pathAndParameters;

        String secret = "ysJ8aZzBsXB4L0wFp1XgQZE2vwfrvaMM";

        Mac hmacSha1 = Mac.getInstance("HmacSHA1");
        hmacSha1.init(new SecretKeySpec(secret.getBytes("UTF-8"), 0, secret.getBytes("UTF-8").length, "HmacSHA1"));
        byte[] md5Result = hmacSha1.doFinal(httpReq.getBytes("UTF-8"));
        String sign = Base64.encodeBase64String(md5Result);

        System.out.println(sign);
    }

    public static void HmacSHA256() throws Exception {
        //这里的变量值如果必须和前端传递过来的参数一致
        String httpMonth = "GET";
        String accept = "*/*";
        String contentMD5 = "";
        String contentType = "";
        String date = "2022-06-14 10:33:00";
        String headers = "x-ca-key:204083472\nx-ca-signature-method:HmacSHA256";
        String pathAndParameters = "/tobImOrderStatistic?endTime=2022-06-07 23:59:59&ownerNo=01&startTime=2022-05-01 00:00:00";

        String[] httpReqArr = {httpMonth,accept,contentMD5,contentType,date,headers,pathAndParameters};
        String httpReq = StringUtils.join(httpReqArr, "\n");

        String secret = "RgvtfzFAW8dhd1UOV6BkuaeVc96Z1xWx";

        Mac hmacSha256 = Mac.getInstance("HmacSHA256");
        byte[] keyBytes = secret.getBytes("UTF-8");
        hmacSha256.init(new SecretKeySpec(keyBytes, 0, keyBytes.length, "HmacSHA256"));
        byte[] md5Result = hmacSha256.doFinal(httpReq.getBytes("UTF-8"));
        String sign = Base64.encodeBase64String(md5Result);

        System.out.println(sign);
    }

    public static void getResponse() throws Exception {
//        OkHttpClient client = new OkHttpClient().newBuilder()
//                .build();
//        MediaType mediaType = MediaType.parse("text/plain");
//        RequestBody body = RequestBody.create(mediaType, "");
//        Request request = new Request.Builder()
//                .url("http://385c3d5d61ab4446a58e8cfcac32d1fd-cn-beijing.alicloudapi.com/tocOrderFailStatistic?end_create_time=2022-06-06&owner_no=01&start_create_time=2022-06-01")
//                .method("GET", body)
//                .addHeader("x-ca-key", "204069071")
//                .addHeader("x-ca-signature-method", "HmacSHA256")
//                .addHeader("x-ca-signature-headers", "x-ca-key,x-ca-signature-method")
//                .addHeader("x-ca-signature", "437Zpo+sYW6Rx2blhm7zc33lYeGBPSE6A6IYL9pFC2A=")
//                .addHeader("date", "2022-06-10 16:18:00")
//                .build();
//        Response response = client.newCall(request).execute();


//        Unirest.setTimeouts(0, 0);
        HttpResponse<String> response = Unirest.get("http://385c3d5d61ab4446a58e8cfcac32d1fd-cn-beijing.alicloudapi.com/tocOrderFailStatistic?end_create_time=2022-06-06&owner_no=01&start_create_time=2022-06-01")
                .header("x-ca-key", "204069071")
                .header("x-ca-signature-method", "HmacSHA256")
                .header("x-ca-signature-headers", "x-ca-key,x-ca-signature-method")
                .header("x-ca-signature", "437Zpo+sYW6Rx2blhm7zc33lYeGBPSE6A6IYL9pFC2A=")
                .header("date", "2022-06-10 16:18:00")
                .accept("*/*")
                .asString();


        System.out.println(response.getHeaders().get("X-Ca-Error-Message"));
    }

    public static void omsOlHmacSHA256() throws Exception {
        //这里的变量值如果必须和前端传递过来的参数一致
        String httpMonth = "GET";
        String accept = "*/*";
        String contentMD5 = "";
        String contentType = "";
        String date = "2022-07-25 10:33:00";
        String headers = "x-ca-key:204083471\nx-ca-signature-method:HmacSHA256";  //开发环境
//        String headers = "x-ca-key:204092252\nx-ca-signature-method:HmacSHA256";  //生产环境
        String pathAndParameters = "/omsOlDeliverInfo?companyId=1&ifDetail=0&orderSubNo=EL05BL2207230063&pageNum=1&pageSize=10";

        String[] httpReqArr = {httpMonth,accept,contentMD5,contentType,date,headers,pathAndParameters};
        String httpReq = StringUtils.join(httpReqArr, "\n");

        String secret = "6TmBmDRzNkUwz7W61T888CMTFs8KvpKj";  //开发环境
//        String secret = "7SiHaEuOhyU2qYe5vetDGP0VpEgmT5ki";  //生产环境

        Mac hmacSha256 = Mac.getInstance("HmacSHA256");
        byte[] keyBytes = secret.getBytes("UTF-8");
        hmacSha256.init(new SecretKeySpec(keyBytes, 0, keyBytes.length, "HmacSHA256"));
        byte[] md5Result = hmacSha256.doFinal(httpReq.getBytes("UTF-8"));
        String sign = Base64.encodeBase64String(md5Result);

        System.out.println(sign);
    }

    public static void olDeliverKpiHmacSHA256() throws Exception {
        //这里的变量值如果必须和前端传递过来的参数一致
        String httpMonth = "GET";
        String accept = "*/*";
        String contentMD5 = "";
        String contentType = "";
        String date = "2022-07-25 10:33:00";
        String headers = "x-ca-key:204083471\nx-ca-signature-method:HmacSHA256";  //开发环境
        //String headers = "x-ca-key:204083472\nx-ca-signature-method:HmacSHA256";  //生产环境
        String pathAndParameters = "/adsLosIntraCityExpressSum?collectType=2&endOrderDate=2022-08-18+23:59:59&pageNum=1&pageSize=25&startOrderDate=2022-08-17+00:00:00";

        // 注意在请求的url路径中年月参数含有中文可能需要进行编码操作, 但是在加密操作中的pathAndParameters还是使用编码前的值
        String encode = URLEncoder.encode("2022年07月", "utf-8");
        System.out.println(encode);
        String decode = URLDecoder.decode("2022%E5%B9%B407%E6%9C%88", "utf-8");
        System.out.println(decode);

        String[] httpReqArr = {httpMonth,accept,contentMD5,contentType,date,headers,pathAndParameters};
        String httpReq = StringUtils.join(httpReqArr, "\n");

        String secret = "6TmBmDRzNkUwz7W61T888CMTFs8KvpKj";  //开发环境
        //String secret = "RgvtfzFAW8dhd1UOV6BkuaeVc96Z1xWx";  //生产环境

        Mac hmacSha256 = Mac.getInstance("HmacSHA256");
        byte[] keyBytes = secret.getBytes("UTF-8");
        hmacSha256.init(new SecretKeySpec(keyBytes, 0, keyBytes.length, "HmacSHA256"));
        byte[] md5Result = hmacSha256.doFinal(httpReq.getBytes("UTF-8"));
        String sign = Base64.encodeBase64String(md5Result);

        System.out.println(sign);
    }

    public static void apiDemoHmacSHA256() throws Exception {
        //这里的变量值如果必须和前端传递过来的参数一致
        String httpMonth = "GET";
        String accept = "*/*";
        String contentMD5 = "";
        String contentType = "";
        String date = "2022-08-02 10:33:00";
        String headers = "x-ca-key:204054476\nx-ca-signature-method:HmacSHA256";  //开发环境
        //String headers = "";  //生产环境  待定
        String pathAndParameters = "/apiDemo?brandDeptName=安德玛&ownerName=拓驰&pageNum=1&pageSize=10";

        // 注意在请求的url路径中品牌部、货主参数含有中文可能需要进行编码操作, 但是在API调用认证操作中的pathAndParameters还是使用编码前的值
        String encode = URLEncoder.encode("安德玛", "utf-8");
        System.out.println(encode);
        String decode = URLDecoder.decode("%E5%AE%89%E5%BE%B7%E7%8E%9B", "utf-8");
        System.out.println(decode);

        String[] httpReqArr = {httpMonth,accept,contentMD5,contentType,date,headers,pathAndParameters};
        String httpReq = StringUtils.join(httpReqArr, "\n");

        String secret = "7mXZdMmiJwF3T3xEolnQsRFCTTAyUXWZ";  //开发环境
        //String secret = "";  //生产环境  待定

        Mac hmacSha256 = Mac.getInstance("HmacSHA256");
        byte[] keyBytes = secret.getBytes("UTF-8");
        hmacSha256.init(new SecretKeySpec(keyBytes, 0, keyBytes.length, "HmacSHA256"));
        byte[] md5Result = hmacSha256.doFinal(httpReq.getBytes("UTF-8"));
        String sign = Base64.encodeBase64String(md5Result);

        System.out.println(sign);
    }

//    public static void omIntransitDaysKpiDemo() {
//        // 设置APP秘钥, 调用域名
//        // 开发环境
//        DwApiClient client = new DwApiClient("204083471", "6TmBmDRzNkUwz7W61T888CMTFs8KvpKj",
//                "35718296901a4fa88e4f4e7f3c5956f1-cn-beijing.alicloudapi.com");
//        // 生产环境
//        // DwApiClient client = new DwApiClient("204083472", "RgvtfzFAW8dhd1UOV6BkuaeVc96Z1xWx",
//        //                                     "35718296901a4fa88e4f4e7f3c5956f1-cn-beijing-vpc.alicloudapi.com");
//        // 构建Request实体
//        OmIntransitDaysKpiRequest omIntransitDaysKpiRequest = new OmIntransitDaysKpiRequest();
//        omIntransitDaysKpiRequest.setYearMonth("2022年08月");
//        omIntransitDaysKpiRequest.setPageNum(1);
//        omIntransitDaysKpiRequest.setPageSize(25);
//        // 发起请求
//        ResponseBody<Paginator<OmIntransitDaysKpi>> response = client.execute(omIntransitDaysKpiRequest);
//        if (response.isSuccess()) {
//            System.out.println(response.getData());
//        } else {
//            System.out.println("code:" + response.getErrCode() + " msg: " + response.getErrMsg());
//        }
//    }





    @Test
    public static void main(String[] args) throws Exception {
//        HmacSHA1();
//        HmacSHA256();
//
//        omsOlHmacSHA256();
//
//        olDeliverKpiHmacSHA256();
//
//        apiDemoHmacSHA256();
//
//        getResponse();
//
//        omIntransitDaysKpiDemo();
//        Demo.expressCollectKpiDemo();
//
//        Demo.lcsOwnerKpiSumDemo();

//        Demo.apiDemo();



//        Demo.winContainerLogInfo();
//        Demo.winTransferInfo();
//        Demo.winTransferInfoSum();\
        Demo.AdsLosIntraCityExpressSumDemo();



    }
}






