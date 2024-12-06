package com.lesoon.bdp_test;

import com.lesoon.bdp.data.service.client.api.MdmOrgRelationshipApi;
import com.lesoon.bdp.data.service.client.dto.MdmOrgRelationshipDto;
import com.lesoon.bdp.data.service.client.vo.MdmOrgRelationshipVo;
import com.lesoon.petrel.common.vo.BizResponseVO;
import org.junit.Test;
import org.junit.runner.RunWith;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.crypto.Mac;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: ApiTest
 * @Description: Api签名测试
 * @Author: zhipengl01
 * @Date: 2022/6/9
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ApiTest {

//    @Autowired
    MdmOrgRelationshipApi mdmOrgRelationship;

    @Test
    public void apiTest() {
        MdmOrgRelationshipVo mdmOrgRelationshipVo = new MdmOrgRelationshipVo();

        mdmOrgRelationshipVo.setAppId("'LESOON496CA60EA45547'");
        mdmOrgRelationshipVo.setCompanyId("1");
        mdmOrgRelationshipVo.setRequestParam("Z0");
        mdmOrgRelationshipVo.setStatus(1);
        mdmOrgRelationshipVo.setPage(1);
        mdmOrgRelationshipVo.setPageSize(5);


        BizResponseVO<List<MdmOrgRelationshipDto>> listBizResponseVO = mdmOrgRelationship.mdmOrgRelationshipRequest(mdmOrgRelationshipVo);
        System.out.println(listBizResponseVO);
    }

//    public static void main(String[] args) {
//        String httpMonth = "GET\n";
//        String accept = "\n";
//        String contentMD5 = "\n";
//        String contentType = "\n";
//        String date = "\n";
//        String headers = "";
//        String pathAndParameters = "/tocOrderDelivery?start_create_time=2022-05-01 00:00:00&end_create_time=2022-06-07 23:59:59&owner_no=01";
//
//        String httpReq = httpMonth+accept+contentMD5+contentType+date+headers+pathAndParameters;
//
////        Mac hmacSHA256 = Mac.getInstance("HmacSHA256");
//
//
//        String old_str = "\"value\"\"value with embedded \\\" quote\"";
//        String pattern = "\"((?:\\\\\"|[^\"]|\\\\\")+)\"";
//
//        Pattern compile = Pattern.compile(pattern);
//
//
//
//
//        Matcher matcher = compile.matcher(old_str);
//
//        while(matcher.find()){
////            System.out.println(matcher.group(0));
//            System.out.println(matcher.group(1));
////            System.out.println(matcher.group(2));
//        }
//
//
//
////        Pattern p = Pattern.compile("\\d{3,5}");
////
////
////        String s = "123-34345-234-00";
////
////        Matcher m = p.matcher(s);
////        while (m.find()) {
////            System.out.println(m.start() + "-" + m.end());
////            System.out.println(m.group());
////        }
//
//
//    }

}
